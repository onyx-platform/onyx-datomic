(defproject com.mdrogalis/onyx-datomic "0.6.0-SNAPSHOT"
  :description "Onyx plugin for Datomic"
  :url "https://github.com/MichaelDrogalis/onyx-datomic"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.mdrogalis/onyx "0.6.0-alpha2"]
                 [com.taoensso/timbre "3.0.1"]]
  :profiles {:dev {:dependencies [[midje "1.6.2"]
                                  [com.datomic/datomic-free "0.9.5153"]]
                   :plugins [[lein-midje "3.1.3"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
