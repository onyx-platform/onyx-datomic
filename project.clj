(defproject org.onyxplatform/onyx-datomic "0.14.5.1"
  :description "Onyx plugin for Datomic"
  :url "https://github.com/onyx-platform/onyx-datomic"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots"       {:url "https://clojars.org/repo"
                                    :username :env
                                    :password :env
                                    :sign-releases false}
                 "releases"         {:url "https://clojars.org/repo"
                                     :username :env
                                     :password :env
                                     :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.14.5"]]
  :test-selectors {:default (complement :ci)
                   :ci :ci
                   :cloud :cloud
                   :all (constantly true)}

  :profiles {:dev {:dependencies [[aero "0.2.0"]
                                  [com.fzakaria/slf4j-timbre "0.3.8"]
                                  [org.slf4j/slf4j-api "1.7.14"]
                                  [org.slf4j/log4j-over-slf4j "1.7.14"]
                                  [org.slf4j/jul-to-slf4j "1.7.14"]
                                  [org.slf4j/jcl-over-slf4j "1.7.14"]]
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]
                   :resource-paths ["test-resources/"]}
             :datomic-peer   {:dependencies [[com.datomic/datomic-free "0.9.5544"]]}
             :datomic-cloud  {:dependencies [[com.datomic/client-cloud "0.8.50"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}
             :default [:base :system :user :provided :dev :datomic-peer]
             :cloud   [:base :system :user :provided :dev :datomic-cloud]})
