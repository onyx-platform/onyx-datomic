(defproject org.onyxplatform/onyx-datomic "0.7.3-SNAPSHOT"
  :description "Onyx plugin for Datomic"
  :url "https://github.com/MichaelDrogalis/onyx-datomic"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.7.3-20150828_160954-g51b5cf8"]]
  :profiles {:dev {:dependencies [[midje "1.7.0"]
                                  [com.datomic/datomic-free "0.9.5153"]]
                   :plugins [[lein-midje "3.1.3"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
