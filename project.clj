(defproject org.onyxplatform/onyx-datomic "0.7.0.7"
  :description "Onyx plugin for Datomic"
  :url "https://github.com/MichaelDrogalis/onyx-datomic"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.onyxplatform/onyx "0.7.2"]]
  :release-tasks [["auto-release" "checkout" "master"]
                  ["auto-release" "merge-no-ff" "develop"]
                  ["vcs" "assert-committed"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["auto-release" "update-release-notes"]
                  ["auto-release" "update-readme-version"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v"]
                  ["deploy" "clojars"]
                  ["vcs" "push"]
                  ["auto-release" "checkout" "develop"]
                  ["auto-release" "merge" "master"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]
  :profiles {:dev {:dependencies [[midje "1.7.0"]
                                  [com.datomic/datomic-free "0.9.5153"]]
                   :plugins [[lein-midje "3.1.3"]
                             [com.andrewmcveigh/lein-auto-release "0.1.10"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
