(ns onyx.plugin.input-log-kill-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin datomic
             [core-async :refer [take-segments!]]
             [core-async-tasks :as core-async]]
            [onyx.tasks.datomic :refer [read-log]]))

(defn build-job [db-uri batch-size batch-timeout]
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
                            (merge {:datomic/uri db-uri
                                    :checkpoint/key "checkpoint"
                                    :checkpoint/force-reset? false
                                    :onyx/max-peers 1}
                                   batch-settings)))
        (add-task (core-async/output-task :persist batch-settings)))))

(defn ensure-datomic!
  ([db-uri data]
   (d/create-database db-uri)
   @(d/transact
     (d/connect db-uri)
     data)))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def people
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Derek"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Kristen"}])

(def people2
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti2"}])

(defn transact-constantly [db-uri]
  (future
    (while (not (Thread/interrupted))
      (Thread/sleep 5)
      (ensure-datomic! db-uri people2))))

(deftest ^:ci datomic-input-log-kill-test
  (let [
        {:keys [env-config peer-config datomic-config]}
        (read-config (clojure.java.io/resource "config.edn") {:profile :test})
        db-uri (str (:datomic-config datomic-config) (java.util.UUID/randomUUID))
        job (build-job db-uri 10 1000)
        {:keys [persist]} (core-async/get-core-async-channels job)
        job-id (atom nil)
        tx-thread (atom nil)]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (testing "That we can read the initial transaction log"
          (mapv (partial ensure-datomic! db-uri) [schema people])
          (reset! job-id (:job-id (onyx.api/submit-job peer-config job)))
          (reset! tx-thread (transact-constantly db-uri))
          (Thread/sleep 5000)
          (onyx.api/kill-job peer-config @job-id)
          (is (not (onyx.api/await-job-completion peer-config @job-id)))
          (swap! tx-thread future-cancel)))
      (finally (d/delete-database db-uri)))))
