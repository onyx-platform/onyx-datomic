(defproject com.mdrogalis/onyx-datomic "0.5.0-SNAPSHOT"
  :description "Onyx plugin for Datomic"
  :url "https://github.com/MichaelDrogalis/onyx-datomic"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.mdrogalis/onyx "0.5.0-SNAPSHOT"]
                 [com.datomic/datomic-free "0.9.4899"]]
  :profiles {:dev {:dependencies [[midje "1.6.2"]
                                  [org.hornetq/hornetq-core-client "2.4.0.Final"]
                                  [com.stuartsierra/component "0.2.1"]]
                   :plugins [[lein-midje "3.1.3"]]}})
