(ns onyx.plugin.tx-async-output-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [>!! close!]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.datomic.api :as d]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [write-bulk-tx-datoms-async]]
             [core-async :as core-async]]))

(defn build-job [datomic-config batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :identity]
                                    [:identity :out]]
                         :catalog [{:onyx/name :identity
                                    :onyx/fn :clojure.core/identity
                                    :onyx/type :function
                                    :onyx/batch-size batch-size}]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (core-async/input :in batch-settings))
        (add-task (write-bulk-tx-datoms-async :out (merge {:datomic/partition :com.mdrogalis/people}
                                                          datomic-config
                                                          batch-settings))))))

(defn ensure-datomic!
  [datomic-config data]
  (d/create-database datomic-config)
  (d/transact
   (d/connect datomic-config)
   data))

(def schema
  [{:db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/ident :name
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/ident :uuid
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def txes
  [{:tx schema}
   {:tx [{:name "Mike" :age 27 :uuid #uuid "f47ac10b-58cc-4372-a567-0e02b2c3d479"}
         {:name "Dorrene" :age 21}
         {:name "Benti" :age 10}
         {:name "Kristen"}
         {:name "Derek"}]}
   {:tx [{:db/id [:name "Mike"] :age 30}]}
   {:tx [[:db/retract [:name "Dorrene"] :age 21]]}
   {:tx [[(d/cas-key) [:name "Benti"] :age 10 18]]}])

(deftest ^:cloud datomic-tx-output-test
  (let [{:keys [env-config peer-config]} (read-config
                                          (clojure.java.io/resource "config.edn")
                                          {:profile :test})
        datomic-config (:datomic-config (read-config
                                         (clojure.java.io/resource "config.edn")
                                         {:profile (d/datomic-lib-type)}))
        db-name (str (java.util.UUID/randomUUID))
        db-uri (str (:datomic/uri datomic-config) db-name)
        datomic-config (assoc datomic-config
                              :datomic/uri db-uri
                              :datomic-cloud/db-name db-name)
        job (build-job datomic-config 10 1000)
        {:keys [in]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (ensure-datomic! datomic-config [])
        (run! (partial >!! in) txes)
        (close! in)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (let [db (d/db (d/connect datomic-config))]
          (is (= (set (map (comp (juxt :name :age :uuid) (partial d/entity db))
                           (apply concat (d/q '[:find ?e :where [?e :name]] db))))
                 #{["Mike" 30 #uuid "f47ac10b-58cc-4372-a567-0e02b2c3d479"]
                   ["Dorrene" nil nil]
                   ["Benti" 18 nil]
                   ["Kristen" nil nil]
                   ["Derek" nil nil]}))))
      (finally (d/delete-database datomic-config)))))
