;;;; Copyright © 2013 Paul Stadig. All rights reserved.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this file,
;;;; You can obtain one at http://mozilla.org/MPL/2.0/.
;;;;
;;;; This Source Code Form is "Incompatible With Secondary Licenses", as defined
;;;; by the Mozilla Public License, v. 2.0.
(ns pjstadig.es
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clj-http.util :refer [url-encode]]
            [slingshot.slingshot :refer [try+]])
  (:import (java.io InputStream PipedInputStream PipedOutputStream)))

(def ^:dynamic *default-http-options* {:content-type :json
                                       :accept :json
                                       :socket-timeout 1000
                                       :conn-timeout 1000
                                       :throw-entire-message? true})

(defn url
  "Constructs an URL by joining base and (URL encoded) parts with '/'."
  [^String base & parts]
  (str (if-not (.endsWith base "/")
         (str base "/")
         base)
       (str/join "/" (map (comp url-encode str) parts))))

(defn parse-body
  "Parses and returns the body of response, if response has a successful status.
  The metadata of the parsed body includes a :response key containing the
  response."
  [response]
  (if (#{200 201} (:status response))
    (try
      (with-meta (let [body (:body response)]
                   (if (instance? InputStream body)
                     (json/decode-stream body)
                     (json/decode body)))
        {:response response})
      (catch Exception e
        response))
    response))

(defmacro log-response [method url response]
  `(let [method# ~method
         url# ~url
         res# ~response]
     (log/trace method# url# "->" res#)
     res#))

(defn http-head
  "Performs a HEAD request against url using http-options, and returns true if
  the response is successful, false if it was not successful, or false if a 404
  was received."
  [url & [http-options]]
  (try+
    (->> (http/head url (merge *default-http-options* http-options))
         (log-response 'HEAD url)
         :status
         (= 200))
    (catch [:status 404] e
      false)))

(defn http-get
  "Performs a GET request against url and returns the parsed body of the
  response.  See parse-body."
  [url & [http-options]]
  (->> (http/get url (merge *default-http-options* http-options))
       (log-response 'GET url)
       parse-body))

(defn http-post
  "Performs a POST request against url and returns the parsed body of the
  response.  See parse-body."
  [url & [http-options]]
  (->> (http/post url (merge *default-http-options* http-options))
       (log-response 'POST url)
       parse-body))

(defn http-put
  "Performs a PUT request against url and returns the parsed body of the
  response.  See parse-body."
  [url & [http-options]]
  (->> (http/put url (merge *default-http-options* http-options))
       (log-response 'PUT url)
       parse-body))

(defn http-delete
  "Performs a DELETE request against url and returns the parsed body of the
  response.  See parse-body."
  [url & [http-options]]
  (->> (http/delete url (merge *default-http-options* http-options))
       (log-response 'DELETE url)
       parse-body))

(defn piped-body
  "Returns a map with a :body key and a value that is a PipedInputStream.  The
  other end of the pipe is wrapped in a writer and passed to f on another
  thread.  When f returns the pipe is automatically closed, and the thread
  dies."
  [f]
  {:pre [f]}
  (let [pis (PipedInputStream.)
        pos (PipedOutputStream.)
        wos (io/writer pos :encoding "utf-8")]
    (.connect pis pos)
    (doto (Thread. #(with-open [wos wos]
                      (try
                        (f wos)
                        (catch Throwable t
                          (log/debug t "piped-body thread died unexpectedly")
                          (throw t)))))
      .start)
    {:body pis}))

(defn json-body
  "Returns a map with a :body key and a value that is a JSON encoded string of
  args.  The arguments should be maps and are merged together before JSON
  encoding."
  [& args]
  {:body (json/encode (apply merge args))})

(defn piped-json-body
  "Returns a map with a :body key and a value that is a PipedInputStream of the
  JSON encoding of args.  The arguments should be maps and are merged together
  before JSON encoding."
  [& args]
  (piped-body #(json/encode-stream (apply merge args) %)))

(defn ok? [response]
  (when (get response "ok")
    response))

(defn valid-index-name? [index-name]
  (when (and (string? index-name) (pos? (count index-name)))
    index-name))

(defn valid-type? [type]
  (when (or (keyword type) (and (string? type) (pos? (count type))))
    type))

(defn valid-id? [id]
  (when (and id (or (not (string? id)) (pos? (count id))))
    id))

(defn index-list [index-names]
  (cond
   (valid-index-name? index-names) [index-names]
   (and (coll? index-names) (every? valid-index-name? index-names)) index-names
   :else (throw (IllegalArgumentException. "Invalid index-names value"))))

(defn index-list-str [index-names]
  (str/join "," (index-list index-names)))

(defn index-exists? [es index-names]
  {:pre [es]}
  (http-head (url es (index-list-str index-names))))

(defn index-create [es index-name & {:keys [mappings settings]}]
  {:pre [es (valid-index-name? index-name)]}
  (ok? (http-post (url es index-name)
                  (json-body (when mappings
                               {:mappings mappings})
                             (when settings
                               {:settings settings})))))

(defn index-refresh [es index-names]
  {:pre [es]}
  (ok? (http-post (url es (index-list-str index-names) "_refresh"))))

(defn index-delete [es index-names]
  {:pre [es]}
  (when (seq index-names)
    (try+
      (ok? (http-delete (url es (index-list-str index-names))))
      (catch [:status 404] _
        true))))

(defn index-settings-get [es index-names]
  {:pre [es]}
  (http-get (url es (index-list-str index-names) "_settings")))

(defn index-settings-put [es index-names settings]
  {:pre [es]}
  (when (map? settings)
    (ok? (http-put (url es (index-list-str index-names) "_settings")
                   (json-body settings)))))

(defn alias-exists? [es alias-name]
  {:pre [es (valid-index-name? alias-name)]}
  (http-head (url es "_alias" alias-name)))

(defn alias-get [es alias-name]
  {:pre [es (valid-index-name? alias-name)]}
  (try+
    (keys (http-get (url es "_alias" alias-name)))
    (catch [:status 404] _
      nil)))

(defn alias-add [es alias-name index-names]
  {:pre [es (valid-index-name? alias-name)]}
  (ok? (http-post (url es "_aliases")
                  (json-body {:actions
                              (for [index-name (index-list index-names)]
                                {:add {:index index-name
                                       :alias alias-name}})}))))

(defn alias-remove [es alias-name index-names]
  {:pre [es (valid-index-name? alias-name)]}
  (ok? (http-delete (url es (index-list-str index-names) "_alias" alias-name))))

(defn alias-delete [es alias-name]
  (let [index-names (alias-get es alias-name)]
    (every? identity (map (partial alias-remove es alias-name)
                          index-names))))

(defn mapping-get [es index-names]
  {:pre [es]}
  (http-get (url es (index-list-str index-names) "_mapping")))

(defn mapping-put [es index-name type mapping]
  {:pre [es (valid-index-name? index-name) (valid-type? type)]}
  (ok? (http-put (url es index-name type "_mapping")
                 (json-body (select-keys mapping [type])))))

(defn search-once* [es index-names query options]
  {:pre [es]}
  (let [search-type (:search-type options)
        search-type (and search-type (name search-type))
        options (dissoc options :search-type)]
    (http-get (url es (index-list-str index-names) "_search")
              (merge (json-body (merge {:query query}
                                       (when (seq options)
                                         options)))
                     (when search-type
                       {:query-params {:search_type search-type}})))))

(defn search-once [es index-names query & {:as options}]
  (search-once* es index-names query options))

(defn search-hits [result]
  (if (map? result)
    (map #(get % "fields") (get-in result ["hits" "hits"]))
    (mapcat search-hits result)))

(defn search* [es index-names query options]
  (let [size (:size options 10000)]
    (letfn [(step [from]
              (lazy-seq
               (let [r (search-once* es index-names query
                                     (assoc options
                                       :from from
                                       :size size))]
                 (when-let [hits (seq (search-hits r))]
                   (cons r (step (+ from (count hits))))))))]
      (step (:from options 0)))))

(defn search [es index-names query & {:as options}]
  (search* es index-names query options))

(defn doc-type [doc]
  (if-let [type (or (:_type doc) (get doc "_type"))]
    (name type)))

(defn doc-id [doc]
  (or (:_id doc) (get doc "_id")))

(defn search-count [es index-names query & {:as options}]
  (search* es index-names query (assoc options :search-type :count)))

(defn search-total [result]
  (get-in result ["hits" "total"]))

(defn doc-create [es index-name doc & {:keys [stream?] :as options}]
  {:pre [(doc-type doc) (or (nil? (doc-id doc)) (valid-id? (doc-id doc)))]}
  (let [body ((if stream? piped-json-body json-body) doc)
        type (doc-type doc)]
    (if-let [id (doc-id doc)]
      (http-put (url es index-name type id "_create") body)
      (http-post (url es index-name type) body))))

(defn doc-index [es index-name doc & {:keys [stream?] :as options}]
  {:pre [(doc-type doc) (valid-id? (doc-id doc))]}
  (let [body ((if stream? piped-json-body json-body) doc)]
    (http-put (url es index-name (doc-type doc) (doc-id doc)) body)))

(defn op-action [op]
  (first (keys op)))

(defn op-source [op]
  (get-in op [(op-action op) :source]))

(defn op-source-meta [op]
  (select-keys (op-source op) [:_index "_index" :_type "_type" :_id "_id"]))

(defn op-prep
  ([op]
     (let [action (op-action op)]
       (update-in op [action] #(merge (op-source-meta op)
                                      %))))
  ([index-name op]
     (let [action (op-action op)]
       (update-in op [action] #(merge (when index-name
                                        {:_index index-name})
                                      (op-source-meta op)
                                      %)))))

(defn op-write! [op wos]
  (let [action (op-action op)
        source (op-source op)
        op (update-in op [action] dissoc :source)
        s (json/encode-stream op wos)]
    (.write wos "\n")
    (when source
      (json/encode-stream source wos)
      (.write wos "\n"))))

(defn doc-bulk
  ([es ops]
     (http-post (url es "_bulk")
                (piped-body #(doseq [op ops]
                               (op-write! (op-prep op) %)))))
  ([es index-name ops]
     (http-post (url es "_bulk")
                (piped-body #(doseq [op ops]
                               (op-write! (op-prep index-name op) %))))))

(defn doc-bulk-create
  ([source]
     (doc-bulk-create (:_type source) (:_id source) source))
  ([type id source]
     (doc-bulk-create (:_index source) type id source))
  ([index-name type id source]
     {:pre [(or (nil? index-name)
                (valid-index-name? index-name))
            (or (nil? (:_index source))
                (and (valid-index-name? (:_index source))
                     (or (nil? index-name)
                         (= index-name (:_index source)))))
            (valid-type? type)
            (or (nil? (:_type source))
                (and (valid-type? (:_type source))
                     (= (name type) (:_type source))))
            (valid-id? id)
            (or (nil? (:_id source))
                (and (valid-id? (:_id source))
                     (= id (:_id source))))
            (nil? (get source "_index"))
            (nil? (get source "_type"))
            (nil? (get source "_id"))]}
     {:create (merge {:_type (name type) :_id id :source source}
                     (when index-name
                       {:_index index-name}))}))

(defn doc-bulk-index
  ([source]
     (doc-bulk-index (:_type source) (:_id source) source))
  ([type id source]
     (doc-bulk-index (:_index source) type id source))
  ([index-name type id source]
     {:pre [(or (nil? index-name)
                (valid-index-name? index-name))
            (or (nil? (:_index source))
                (and (valid-index-name? (:_index source))
                     (or (nil? index-name)
                         (= index-name (:_index source)))))
            (valid-type? type)
            (or (nil? (:_type source))
                (and (valid-type? (:_type source))
                     (= (name type) (:_type source))))
            (valid-id? id)
            (or (nil? (:_id source))
                (and (valid-id? (:_id source))
                     (= id (:_id source))))
            (nil? (get source "_index"))
            (nil? (get source "_type"))
            (nil? (get source "_id"))]}
     {:index (merge {:_type (name type) :_id id :source source}
                    (when index-name
                      {:_index index-name}))}))

(defn doc-bulk-delete
  ([type id]
     {:pre [(valid-type? type) (valid-id? id)]}
     {:delete {:_type type :_id id}})
  ([index-name type id]
     {:pre [(valid-index-name? index-name) (valid-type? type) (valid-id? id)]}
     {:delete {:_index index-name :_type type :_id id}}))
