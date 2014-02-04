;;;; Copyright © 2013-2014 Paul Stadig. All rights reserved.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this file,
;;;; You can obtain one at http://mozilla.org/MPL/2.0/.
;;;;
;;;; This Source Code Form is "Incompatible With Secondary Licenses", as defined
;;;; by the Mozilla Public License, v. 2.0.
(ns pjstadig.test.es
  (:require [clojure.test :refer :all]
            [pjstadig.es :refer :all]
            [pjstadig.es.query :as q]
            [pjstadig.util :refer [uuid-hex-str uuid-url-str]]))

(def es "http://localhost:9200/")
(defonce index-name0 (uuid-hex-str))

(use-fixtures :once
  (fn check-elasticsearch [f]
    (try
      (when (http-get es)
        (f))
      (catch Throwable e
        (println "*** WARNING: Skipping tests, because Elasticsearch could not"
                 "be reached"))))
  (fn print-default-index [f]
    (println "*** Test index:" index-name0)
    (f)))

(use-fixtures :each
  (fn default-http-options [f]
    (binding [*default-http-options* (assoc *default-http-options*
                                       :throw-entire-message? true)]
      (f)))
  (fn default-index [f]
    (if (index-exists? es index-name0)
      (index-delete es index-name0))
    (f)))

(deftest test-index-ops
  (is (not (index-exists? es index-name0)))
  (index-create es index-name0
                :settings {"index" {"number_of_shards" 6
                                    "number_of_replicas" 3}})
  (is (index-exists? es index-name0))
  (is (= {index-name0
          {"settings"
           {"index.number_of_shards" "6"
            "index.number_of_replicas" "3"}}}
         (update-in (index-settings-get es index-name0) [index-name0 "settings"]
                    dissoc "index.uuid" "index.version.created")))
  (is (index-settings-put es index-name0
                          {"index" {"number_of_replicas" "2"}}))
  (is (= {index-name0
          {"settings"
           {"index.number_of_shards" "6"
            "index.number_of_replicas" "2"}}}
         (update-in (index-settings-get es index-name0) [index-name0 "settings"]
                    dissoc "index.uuid" "index.version.created")))
  (is (index-delete es index-name0))
  (is (not (index-exists? es index-name0)))
  (let [index-name1 (uuid-hex-str)]
    (is (index-create es index-name0))
    (is (index-create es index-name1))
    (is (index-delete es [index-name0 index-name1]))
    (is (not (index-exists? es index-name0)))
    (is (not (index-exists? es index-name1)))))

(deftest test-alias-ops
  (let [index-name1 (uuid-hex-str)
        alias-name (uuid-hex-str)]
    (index-create es index-name0)
    (index-create es index-name1)
    (is (not (alias-exists? es alias-name)))
    (is (alias-add es alias-name [index-name0 index-name1]))
    (is (alias-exists? es alias-name))
    (is (index-exists? es alias-name))
    (is (not (alias-exists? es index-name0)))
    (is (= #{index-name0 index-name1}
           (set (alias-get es alias-name))))
    (is (alias-remove es alias-name [index-name0]))
    (is (= #{index-name1}
           (set (alias-get es alias-name))))
    (is (alias-delete es alias-name))
    (is (not (alias-exists? es alias-name)))
    (is (nil? (alias-get es alias-name)))
    (is (index-exists? es index-name1))
    (is (index-delete es index-name1))))

(deftest test-mapping-ops
  (let [mappings {"0" {"properties" {"subject" {"type" "string"}}}}]
    (index-create es index-name0 :mappings mappings)
    (is (= {index-name0 mappings} (mapping-get es index-name0)))
    (is (mapping-put es index-name0 "0" {"0"
                                         {"properties"
                                          {"foo"
                                           {"type" "string"}}}}))
    (is (= {index-name0 (assoc-in mappings ["0" "properties" "foo"]
                                  {"type" "string"})}
           (mapping-get es index-name0)))))

(deftest test-doc-ops
  (let [mappings {"0" {"properties" {"subject" {"type" "string"
                                                "index" "not_analyzed"}
                                     "body" {"type" "string"
                                             "index" "not_analyzed"}}}}
        id0 (uuid-url-str)
        id1 (uuid-url-str)
        id2 (uuid-url-str)
        subject "subject"
        body "body"]
    (index-create es index-name0 :mappings mappings)
    (is (= id0
           (get (doc-create es index-name0
                            {:_type "0" :_id id0 :subject subject :body body})
                "_id")))
    (index-refresh es [index-name0])
    (is (= [{"_id" id0 "subject" subject "body" body}]
           (search-hits (search es index-name0
                                (q/term :subject subject)
                                :fields ["_id" "subject" "body"]))))
    (is (ok? (doc-index es index-name0
                        {:_type "0" :_id id0 :subject subject :body "foo"})))
    (index-refresh es [index-name0])
    (is (= [{"_id" id0 "subject" subject "body" "foo"}]
           (search-hits (search es index-name0
                                (q/term :subject subject)
                                :fields ["_id" "subject" "body"]))))
    (is (ok? (doc-delete es index-name0 "0" id0)))
    (index-refresh es [index-name0])
    (is (empty? (search-hits (search es index-name0
                                     (q/term :subject subject)
                                     :fields ["_id" "subject" "body"]))))))

(defn bulk-ok? [response]
  (every? (comp ok? second first) (get response ["items"])))

(deftest test-bulk
  (let [mappings {"0" {"properties" {"subject" {"type" "string"
                                                "index" "not_analyzed"}
                                     "body" {"type" "string"
                                             "index" "not_analyzed"}}}}
        id0 (uuid-url-str)
        id1 (uuid-url-str)
        subject "subject"
        body "body"]
    (index-create es index-name0 :mappings mappings)
    (is (bulk-ok? (doc-bulk es (bulk-create-ops [{:_index index-name0
                                                  :_type "0"
                                                  :_id id0
                                                  :subject subject
                                                  :body body}]))))
    (index-refresh es [index-name0])
    (is (= #{{"_id" id0 "subject" subject "body" body}}
           (set (search-hits (search es index-name0
                                     (q/term :subject subject)
                                     :fields ["_id" "subject" "body"])))))
    (is (bulk-ok? (doc-bulk-for-index es index-name0
                                      (bulk-create-ops [{:_type "0"
                                                         :_id id1
                                                         :subject subject
                                                         :body body}]))))
    (index-refresh es [index-name0])
    (is (= #{{"_id" id0 "subject" subject "body" body}
             {"_id" id1 "subject" subject "body" body}}
           (set (search-hits (search es index-name0
                                     (q/term :subject subject)
                                     :fields ["_id" "subject" "body"])))))))

(deftest test-search-hits
  (let [mappings {"0" {"properties" {"subject" {"type" "string"
                                                "index" "not_analyzed"}
                                     "body" {"type" "string"
                                             "index" "not_analyzed"}}}}
        id0 (uuid-url-str)
        id1 (uuid-url-str)
        id2 (uuid-url-str)
        subject "subject"
        body "body"]
    (index-create es index-name0 :mappings mappings)
    (is (= id0
           (get (doc-create es index-name0
                            {:_type "0" :_id id0 :subject subject :body body})
                "_id")))
    (index-refresh es [index-name0])
    (is (= [{"_id" id0 "subject" subject "body" body}]
           (search-hits (search es index-name0
                                (q/term :subject subject)
                                :fields ["_id" "subject" "body"]))))
    (is (= [{"_type" "0", "_id" id0, "subject" subject "body" body}]
           (search-hits (search es index-name0
                                (q/term :subject subject)))))))
