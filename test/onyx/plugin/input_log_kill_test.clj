(ns onyx.plugin.input-log-kill-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is testing]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.datomic.api :as d]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [read-log]]
             [core-async :as core-async]]))

(defn build-job [datomic-config batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-log :persist]]
                         :catalog []
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (read-log :read-log
                            (merge {:checkpoint/key "checkpoint"
                                    :checkpoint/force-reset? false
                                    :onyx/max-peers 1}
                                   datomic-config
                                   batch-settings)))
        (add-task (core-async/output :persist batch-settings)))))

(defn ensure-datomic!
  ([datomic-config data]
   (d/create-database datomic-config)
   (d/transact
    (d/connect datomic-config)
    data)))

(def schema
  [{:db/ident :com.mdrogalis/people}

   {:db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(def people
  [{:user/name "Mike"}
   {:user/name "Dorrene"}
   {:user/name "Benti"}
   {:user/name "Derek"}
   {:user/name "Kristen"}])

(def people2
  [{:user/name "Mike2"}
   {:user/name "Dorrene2"}
   {:user/name "Benti2"}])

(defn transact-constantly [datomic-config]
  (future
    (while (not (Thread/interrupted))
      (Thread/sleep 5)
      (ensure-datomic! datomic-config people2))))

(deftest ^:ci ^:cloud datomic-input-log-kill-test
  (let [{:keys [env-config peer-config]}
        (read-config (clojure.java.io/resource "config.edn") {:profile :test})
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
        {:keys [persist]} (get-core-async-channels job)
        job-id (atom nil)
        tx-thread (atom nil)]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (testing "That we can read the initial transaction log"
          (mapv (partial ensure-datomic! datomic-config) [schema people])
          (reset! job-id (:job-id (onyx.api/submit-job peer-config job)))
          (reset! tx-thread (transact-constantly db-uri))
          (Thread/sleep 5000)
          (onyx.api/kill-job peer-config @job-id)
          (is (not (onyx.api/await-job-completion peer-config @job-id)))
          (swap! tx-thread future-cancel)))
      (finally (d/delete-database datomic-config)))))
