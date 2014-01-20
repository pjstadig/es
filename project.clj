(defproject pjstadig/es "0.1.0-SNAPSHOT"
  :description "A simple REST-based Elasticsearch client for Clojure."
  :url "http://github.com/pjstadig/es"
  :license {:name "Mozilla Public License, v. 2.0"
            :url "http://mozilla.org/MPL/2.0/"}
  :dependencies [[cheshire "5.2.0"]
                 [clj-http "0.7.7"]
                 [log4j "1.2.17"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [slingshot "0.10.3"]]
  ;;:global-vars {*warn-on-reflection* true}
  )