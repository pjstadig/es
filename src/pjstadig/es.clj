;;;; Copyright © 2013-2014 Paul Stadig. All rights reserved.
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
    (doto (Thread. (bound-fn []
                     (with-open [wos wos]
                       (try
                         (f wos)
                         (catch Throwable t
                           (log/debug t "piped-body thread died unexpectedly")
                           (throw t))))))
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

(defn ok?
  "True if the response has \"ok\" true"
  [response]
  (when (get response "ok")
    response))

(defn valid-index-name?
  "True if index-name is a non-empty string"
  [index-name]
  (when (and (string? index-name) (pos? (count index-name)))
    index-name))

(defn valid-type-name?
  "True if type-name is a keyword or non-empty string"
  [type-name]
  (when (or (keyword type-name)
            (and (string? type-name)
                 (pos? (count type-name))))
    type-name))

(defn valid-id?
  "True if id is a keyword, symbol, or non-empty string"
  [id]
  (when (and id (or (not (string? id)) (pos? (count id))))
    id))

(defn index-list
  "If index-names is a single valid index-name, then return a sequence including
  just that index-name.  If index-names is a sequence of valid-names, then
  return index-names.  Otherwise throws an exception."
  [index-names]
  (cond
   (valid-index-name? index-names) [index-names]
   (and (coll? index-names) (every? valid-index-name? index-names)) index-names
   :else (throw (IllegalArgumentException. "Invalid index-names value"))))

(defn index-list-str
  "A string of comma separated index-names where index-names is filtered through
  index-list."
  [index-names]
  (str/join "," (index-list index-names)))

(defn index-exists?
  "True if index-names exist, false otherwise"
  [es index-names]
  {:pre [es]}
  (http-head (url es (index-list-str index-names))))

(defn index-create
  "Creates index-name given mappings and settings."
  [es index-name & {:keys [mappings settings]}]
  {:pre [es (valid-index-name? index-name)]}
  (ok? (http-post (url es index-name)
                  (json-body (when mappings
                               {:mappings mappings})
                             (when settings
                               {:settings settings})))))

(defn index-refresh
  "Refreshes index-names."
  [es index-names]
  {:pre [es]}
  (ok? (http-post (url es (index-list-str index-names) "_refresh"))))

(defn index-delete
  "Delete index-names."
  [es index-names]
  {:pre [es]}
  (when (seq index-names)
    (try+
      (ok? (http-delete (url es (index-list-str index-names))))
      (catch [:status 404] _
        true))))

(defn index-settings-get
  "Retrieves the settings for index-names."
  [es index-names]
  {:pre [es]}
  (http-get (url es (index-list-str index-names) "_settings")))

(defn index-settings-put
  "Puts settings for index-names."
  [es index-names settings]
  {:pre [es]}
  (when (map? settings)
    (ok? (http-put (url es (index-list-str index-names) "_settings")
                   (json-body settings)))))

(defn alias-exists?
  "True if alias-name exists."
  [es alias-name]
  {:pre [es (valid-index-name? alias-name)]}
  (http-head (url es "_alias" alias-name)))

(defn alias-get
  "Returns list of index names that are a part of alias-name."
  [es alias-name]
  {:pre [es (valid-index-name? alias-name)]}
  (try+
    (keys (http-get (url es "_alias" alias-name)))
    (catch [:status 404] _
      nil)))

(defn alias-add
  "Adds index-names to alias-name."
  [es alias-name index-names]
  {:pre [es (valid-index-name? alias-name)]}
  (ok? (http-post (url es "_aliases")
                  (json-body {:actions
                              (for [index-name (index-list index-names)]
                                {:add {:index index-name
                                       :alias alias-name}})}))))

(defn alias-remove
  "Removes index-names from alias-name."
  [es alias-name index-names]
  {:pre [es (valid-index-name? alias-name)]}
  (ok? (http-delete (url es (index-list-str index-names) "_alias" alias-name))))

(defn alias-delete
  "Deletes alias-name."
  [es alias-name]
  (let [index-names (alias-get es alias-name)]
    (every? identity (map (partial alias-remove es alias-name)
                          index-names))))

(defn mapping-get
  "Returns mappings for index-names."
  [es index-names]
  {:pre [es]}
  (http-get (url es (index-list-str index-names) "_mapping")))

(defn mapping-put
  "Puts mapping for index-name and type-name."
  [es index-name type-name mapping]
  {:pre [es (valid-index-name? index-name) (valid-type-name? type-name)]}
  (ok? (http-put (url es index-name type-name "_mapping")
                 (json-body (select-keys mapping [type-name])))))

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

(defn search-once
  "Returns a raw results for query in index-names.  Hits can be accessed using
  search-hits."
  [es index-names query & {:as options}]
  (search-once* es index-names query options))

(defn search-hits [result]
  (if (map? result)
    (map #(or (get % "_source")
              (get % "fields"))
         (get-in result ["hits" "hits"]))
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

(defn search
  "Returns a lazy sequence of results for query in index-names."
  [es index-names query & {:as options}]
  (search* es index-names query options))

(defn doc-type
  "Gets the type-name (via :_type or \"_type\") from doc."
  [doc]
  (if-let [type-name (or (:_type doc) (get doc "_type"))]
    (name type-name)))

(defn doc-id
  "Gets the id (via :_id or \"_id\") from doc."
  [doc]
  (or (:_id doc) (get doc "_id")))

(defn search-count
  "Returns count of the number of hits that match query (without actually
  returning the hits)."
  [es index-names query & {:as options}]
  (search* es index-names query (assoc options :search-type :count)))

(defn search-total
  "Returns the total number of hits from result."
  [result]
  (get-in result ["hits" "total"]))

(defn doc-create
  "Creates doc in index-name using the ES create operation.  Will stream data to
  ES if :stream? is truthy."
  [es index-name doc & {:keys [stream?] :as options}]
  {:pre [(doc-type doc) (or (nil? (doc-id doc)) (valid-id? (doc-id doc)))]}
  (let [body ((if stream? piped-json-body json-body) doc)
        type-name (doc-type doc)]
    (if-let [id (doc-id doc)]
      (http-put (url es index-name type-name id "_create") body)
      (http-post (url es index-name type-name) body))))

(defn doc-index
  "Index doc in index-name using the ES index operation (creates it if it
  doesn't exist, or deletes it and re-creates it, if it does).  Will stream data
  to ES if the :stream? option is truthy."
  [es index-name doc & {:keys [stream?] :as options}]
  {:pre [(doc-type doc) (valid-id? (doc-id doc))]}
  (let [body ((if stream? piped-json-body json-body) doc)]
    (http-put (url es index-name (doc-type doc) (doc-id doc)) body)))

(defn doc-delete
  "Delete doc from index-name.  Will stream data to ES if the :stream? option is
  truthy."
  [es index-name type-name id & {:keys [stream?] :as options}]
  {:pre [(valid-index-name? index-name) (valid-type-name? type-name)
         (valid-id? id)]}
  (let [body ((if stream? piped-json-body json-body) {:_index index-name
                                                      :_type type-name
                                                      :_id id})]
    (http-delete (url es index-name type-name id) body)))

(defn op-action
  "Returns the action from op (its first and only key)."
  [op]
  (first (keys op)))

(defn op-source
  "Returns the source from op."
  [op]
  (get-in op [(op-action op) :source]))

(defn op-source-meta
  "Returns the meta from the source of the op (the _index, _type, and _id
  keys)."
  [op]
  (select-keys (op-source op) [:_index "_index" :_type "_type" :_id "_id"]))

(defn op-prep
  "Prep op to be sent to ES.  It merges the meta from the source of the op into
  the op, and merges in the index-name if it is specified."
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

(defn op-write!
  "Writes JSON encoded op to writer."
  [op wos]
  (let [action (op-action op)
        source (op-source op)
        op (update-in op [action] dissoc :source)
        s (json/encode-stream op wos)]
    (.write wos "\n")
    (when source
      (json/encode-stream source wos)
      (.write wos "\n"))))

(defn doc-bulk
  "Performs bulk index operations."
  ([es ops]
     (http-post (url es "_bulk")
                (piped-body #(doseq [op ops]
                               (op-write! (op-prep op) %)))))
  ([es index-name ops]
     (http-post (url es "_bulk")
                (piped-body #(doseq [op ops]
                               (op-write! (op-prep index-name op) %))))))

(defn bulk-write! [metas&sources writer]
  (doseq [doc metas&sources]
    (json/encode-stream doc writer)
    (.write writer "\n")))

(defn bulk-post
  ([url metas&sources]
     (http-post url (piped-body (partial bulk-write! metas&sources))))
  ([url metas&sources params]
     (http-post url (assoc (piped-body (partial bulk-write! metas&sources))
                      :query-params params))))

(defn doc-bulk
  "Performs bulk operations."
  ([es metas&sources]
     (bulk-post (url es "_bulk") metas&sources))
  ([es metas&sources params]
     (bulk-post (url es "_bulk") metas&sources params)))

(defn doc-bulk-for-index
  "Performs bulk operations against index-name."
  ([es index-name metas&sources]
     (bulk-post (url es index-name "_bulk") metas&sources))
  ([es index-name metas&sources params]
     (bulk-post (url es index-name "_bulk") metas&sources params)))

(defn doc-bulk-for-index-and-type
  "Performs bulk operations against index-name and type-name."
  ([es index-name type-name metas&sources]
     (bulk-post (url es index-name type-name "_bulk") metas&sources))
  ([es index-name type-name metas&sources params]
     (bulk-post (url es index-name type-name "_bulk") metas&sources)))

(defn select-keys-or-strs [m ks]
  (reduce (fn [r k]
            (if (contains? m k)
              (assoc r k (k m))
              (let [s (name k)]
                (if (contains? m s)
                  (assoc r k (get m s))
                  r))))
          {}
          ks))

(defn bulk-create-ops [sources]
  (mapcat (fn [source]
            [{:create (select-keys-or-strs source [:_index :_type :_id])}
             source])
          sources))

(defn bulk-index-ops [sources]
  (mapcat (fn [source]
            [{:index (select-keys-or-strs source [:_index :_type :_id])}
             source])
          sources))

(defn bulk-update-ops [sources]
  (mapcat (fn [source]
            [{:update (select-keys-or-strs source [:_index :_type :_id])}
             source])
          sources))
