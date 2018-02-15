(ns onyx.plugin.output-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [close! >!!]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.datomic.api :as d]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [write-datoms]]
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
        (add-task (write-datoms :out (merge {:datomic/partition :com.mdrogalis/people}
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
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def people
  [{:name "Mike"}
   {:name "Dorrene"}
   {:name "Benti"}
   {:name "Kristen"}
   {:name "Derek"}])

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
        datomic-config (if (string? (:datomic-cloud/proxy-port datomic-config))
                         (assoc datomic-config
                                :datomic-cloud/proxy-port (Integer/parseInt
                                                           (:datomic-cloud/proxy-port datomic-config)))
                         datomic-config)
        job (build-job datomic-config 10 1000)
        {:keys [in]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (ensure-datomic! datomic-config schema)
        (run! (partial >!! in) people)
        (close! in)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.test-helper/feedback-exception! peer-config))
        (let [db (d/db (d/connect datomic-config))]
          (is (= (set (remove nil? (map :name people)))
                 (set (apply concat (d/q '[:find ?a :where [_ :name ?a]] db)))))))
      (finally (d/delete-database datomic-config)))))
