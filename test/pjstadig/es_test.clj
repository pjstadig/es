;;;; Copyright © 2013-2014 Paul Stadig. All rights reserved.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this file,
;;;; You can obtain one at http://mozilla.org/MPL/2.0/.
;;;;
;;;; This Source Code Form is "Incompatible With Secondary Licenses", as defined
;;;; by the Mozilla Public License, v. 2.0.
(ns pjstadig.test.es-test
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
  (let [query #(->> (q/term :subject "subject")
                    (search es index-name0)
                    search-hits
                    set)
        mappings {"0" {"properties" {"subject" {"type" "string"
                                                "index" "not_analyzed"}
                                     "body" {"type" "string"
                                             "index" "not_analyzed"}}}}
        make-doc #(hash-map "_index" index-name0
                            "_type" "0"
                            "_id" (uuid-url-str)
                            "subject" "subject"
                            "body" "body")
        doc0 (make-doc)
        doc1 (make-doc)
        doc2 (make-doc)]
    (index-create es index-name0 :mappings mappings)
    (testing "create operations"
      (is (->> (bulk-create-ops [doc0])
               (doc-bulk es)
               bulk-ok?))
      (index-refresh es [index-name0])
      (is (= #{doc0} (query)))
      (is (->> (bulk-create-ops [doc1])
               (doc-bulk-for-index es index-name0)
               bulk-ok?))
      (index-refresh es [index-name0])
      (is (= #{doc0 doc1} (query)))
      (is (->> (bulk-create-ops [doc2])
               (doc-bulk-for-index-and-type es index-name0 "0")
               bulk-ok?))
      (index-refresh es [index-name0])
      (is (= #{doc0 doc1 doc2} (query))))
    (testing "index operations (and refresh param)"
      (is (->> (bulk-index-ops (map #(assoc % "body" "body1")
                                    [doc0 doc1 doc2]))
               (#(doc-bulk es % {:refresh true}))
               bulk-ok?))
      (is (= (set (map #(assoc % "body" "body1") [doc0 doc1 doc2])) (query)))
      (is (->> (bulk-index-ops (map #(assoc % "body" "body2") [doc0 doc1 doc2]))
               (#(doc-bulk-for-index es index-name0 % {:refresh true}))
               bulk-ok?))
      (is (= (set (map #(assoc % "body" "body2") [doc0 doc1 doc2])) (query)))
      (is (->> (bulk-index-ops (map #(assoc % "body" "body3") [doc0 doc1 doc2]))
               (#(doc-bulk-for-index-and-type es index-name0 "0" %
                                              {:refresh true}))
               bulk-ok?))
      (is (= (set (map #(assoc % "body" "body3") [doc0 doc1 doc2])) (query))))
    (testing "update operations"
      (let [make-update (fn [body]
                          (fn [source]
                            (merge (select-keys source ["_index" "_type" "_id"])
                                   {"script"
                                    (str "ctx._source.body=\"" body "\"")
                                    "_retry_on_conflict" "2"})))]
        (is (->> (bulk-update-ops (map (make-update "body2") [doc0 doc1 doc2]))
                 (doc-bulk es)
                 bulk-ok?))
        (index-refresh es [index-name0])
        (is (= (set (map #(assoc % "body" "body2") [doc0 doc1 doc2])) (query)))
        (is (->> (bulk-update-ops (map (make-update "body1") [doc0 doc1 doc2]))
                 (doc-bulk-for-index es index-name0)
                 bulk-ok?))
        (index-refresh es [index-name0])
        (is (= (set (map #(assoc % "body" "body1") [doc0 doc1 doc2])) (query)))
        (is (->> (bulk-update-ops (map (make-update "body") [doc0 doc1 doc2]))
                 (doc-bulk-for-index-and-type es index-name0 "0")
                 bulk-ok?))
        (index-refresh es [index-name0])
        (is (= #{doc0 doc1 doc2} (query)))))
    (testing "delete operations"
      (is (->> (bulk-delete-ops [doc0])
               (doc-bulk es)
               bulk-ok?))
      (index-refresh es index-name0)
      (is (= (set [doc1 doc2]) (query)))
      (is (->> (bulk-delete-ops [doc1])
               (doc-bulk-for-index es index-name0)
               bulk-ok?))
      (index-refresh es index-name0)
      (is (= (set [doc2]) (query)))
      (is (->> (bulk-delete-ops [doc2])
               (doc-bulk-for-index-and-type es index-name0 "0")
               bulk-ok?))
      (index-refresh es index-name0)
      (is (empty? (query))))))

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

(deftest test-msearch
  (let [mappings {"0" {"properties" {"subject" {"type" "string"
                                                "index" "not_analyzed"}
                                     "body" {"type" "string"
                                             "index" "not_analyzed"}}}}
        make-doc #(hash-map "_index" index-name0
                            "_type" "0"
                            "_id" (uuid-url-str)
                            "subject" "subject"
                            "body" "body")
        doc0 (make-doc)
        doc1 (make-doc)
        doc2 (make-doc)
        index-name1 (uuid-hex-str)
        doc3 (assoc (make-doc) "_index" index-name1)]
    (index-create es index-name0 :mappings mappings)
    (is (bulk-ok? (doc-bulk es (bulk-create-ops [doc0 doc1 doc2]))))
    (index-create es index-name1 :mappings mappings)
    (is (bulk-ok? (doc-bulk es (bulk-create-ops [doc3]))))
    (index-refresh es [index-name0 index-name1])
    (is (= [#{doc0 doc1 doc2} #{doc3}]
           (map (comp set search-hits)
                (get (->> (bulk-queries [{:query { :term {:body "body"}}
                                          :index index-name0
                                          :type "0"}
                                         {:query {:term {:subject "subject"}}
                                          :index index-name1}])
                          (msearch es))
                     "responses"))))
    (is (= [#{doc0 doc1 doc2} #{doc3}]
           (map (comp set search-hits)
                (get (->> (bulk-queries [{:query {:term {:body "body"}}
                                          :type "0"}
                                         {:query {:term {:subject "subject"}}
                                          :index index-name1}])
                          (msearch-for-index es index-name0))
                     "responses"))))
    (is (= [#{doc0 doc1 doc2} #{doc3}]
           (map (comp set search-hits)
                (get (->> (bulk-queries [{:query {:term {:body "body"}}}
                                         {:query {:term {:subject "subject"}}
                                          :index index-name1}])
                          (msearch-for-index-and-type es index-name0 "0"))
                     "responses"))))))

(deftest test-mget
  (let [mappings {"0" {"properties" {"subject" {"type" "string"
                                                "index" "not_analyzed"}
                                     "body" {"type" "string"
                                             "index" "not_analyzed"}}}}
        make-doc #(hash-map "_index" index-name0
                            "_type" "0"
                            "_id" (uuid-url-str)
                            "subject" "subject"
                            "body" "body")
        doc0 (make-doc)
        doc1 (make-doc)
        doc2 (make-doc)
        index-name1 (uuid-hex-str)
        doc3 (assoc (make-doc) "_index" index-name1)]
    (index-create es index-name0 :mappings mappings)
    (is (bulk-ok? (doc-bulk es (bulk-create-ops [doc0 doc1 doc2]))))
    (index-create es index-name1 :mappings mappings)
    (is (bulk-ok? (doc-bulk es (bulk-create-ops [doc3]))))
    (index-refresh es [index-name0 index-name1])
    (is (= #{doc0 doc1 doc2 doc3}
           (set (map #(get % "_source")
                     (get (mget es (map #(select-keys % ["_index" "_type"
                                                         "_id"])
                                        [doc0 doc1 doc2 doc3])) "docs")))))
    (is (= #{doc0 doc1 doc2}
           (set (map #(get % "_source")
                     (get (mget-for-index es index-name0
                                          (map #(select-keys % ["_type" "_id"])
                                               [doc0 doc1 doc2]))
                          "docs")))))
    (is (= #{doc0 doc1 doc2}
           (set (map #(get % "_source")
                     (get (mget-for-index-and-type es index-name0 "0"
                                                   (map #(select-keys % ["_id"])
                                                        [doc0 doc1 doc2]))
                          "docs")))))
    (is (= #{doc0 doc1 doc2}
           (set (map #(get % "_source")
                     (get (mget-for-index-and-type es index-name0 "0"
                                                   (map #(get % "_id")
                                                        [doc0 doc1 doc2]))
                          "docs")))))))
