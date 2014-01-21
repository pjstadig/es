;;;; Copyright © 2013 Paul Stadig. All rights reserved.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this file,
;;;; You can obtain one at http://mozilla.org/MPL/2.0/.
;;;;
;;;; This Source Code Form is "Incompatible With Secondary Licenses", as defined
;;;; by the Mozilla Public License, v. 2.0.
(ns pjstadig.es.query)

(defn term [field term]
  {:pre [field term]}
  {:term {field term}})

(defn match [field query]
  {:pre [field query]}
  {:match {field query}})

(defn match-all []
  {:match_all {}})

(defn bool [& {:keys [must should must-not]}]
  {:pre [(or (seq must) (seq should) (seq must-not))]}
  {:bool (merge (when (seq must)
                  {:must must})
                (when (seq should)
                  {:should should})
                (when (seq must-not)
                  {:must-not must-not}))})

(defn filtered [query filter]
  {:pre [query filter]}
  {:filtered {:query query
              :filter filter}})

(defn range [field & {:keys [lt lte gt gte]}]
  {:pre [field (or lt lte gt gte)]}
  {:range {field (merge (when lt
                          {:lt lt})
                        (when lte
                          {:lte lte})
                        (when gt
                          {:gt gt})
                        (when gte
                          {:gte gte}))}})

(defn ids [ids]
  {:pre [(seq ids)]}
  {:ids {:values ids}})

(defn not [filter]
  {:not filter})

(defn terms [field terms & {:keys [execution]}]
  {:pre [(contains? #{:plain :bool :and :or} execution)]}
  {:terms {field terms :execution execution}})
