(ns onyx.plugin.input-log-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [read-log]]
             [core-async :as core-async]]))

(defn build-job [db-uri log-end-tx batch-size batch-timeout]
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
                                    :onyx/max-peers 1
                                    :datomic/log-end-tx log-end-tx}
                                   batch-settings)))
        (add-task (core-async/output :persist batch-settings 100000)))))

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

(def people3
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike3"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene3"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti3"}])

(def people4
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike4"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene4"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti4"}])

(deftest datomic-input-log-test
  (let [{:keys [env-config peer-config datomic-config]}
        (read-config (clojure.java.io/resource "config.edn") {:profile :test})
        db-uri (str "datomic:mem://" (java.util.UUID/randomUUID))]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (testing "That we can read the initial transaction log"
          (let [job (build-job db-uri 1002 10 1000)
                {:keys [persist]} (get-core-async-channels job)
                _ (mapv (partial ensure-datomic! db-uri) [schema people])
                job-id (:job-id (onyx.api/submit-job peer-config job))]
            (ensure-datomic! db-uri people2)
            (onyx.api/await-job-completion peer-config job-id)
            (is (= [{:data '([63 10 :com.mdrogalis/people 13194139534312 true]
                             [0 11 63 13194139534312 true]
                             [64 10 :user/name 13194139534312 true]
                             [64 40 23 13194139534312 true]
                             [64 41 35 13194139534312 true]
                             [0 13 64 13194139534312 true]) :t 1000}
                    {:data '([277076930200554 64 "Mike" 13194139534313 true]
                             [277076930200555 64 "Dorrene" 13194139534313 true]
                             [277076930200556 64 "Benti" 13194139534313 true]
                             [277076930200557 64 "Derek" 13194139534313 true]
                             [277076930200558 64 "Kristen" 13194139534313 true]) :t 1001}]
                   (map (fn [result]
                          (-> result
                              (update :data rest)
                              (dissoc :id)))
                        (take-segments! persist 50))))
            ;; recover from resume point, should only get the latest txes
            (let [new-job (build-job db-uri 1014 10 1000)
                  job2 (->> job-id
                            (onyx.api/job-snapshot-coordinates peer-config (:onyx/tenancy-id peer-config))
                            (onyx.api/build-resume-point new-job)
                            (assoc new-job :resume-point))
                  {:keys [persist]} (get-core-async-channels job2)
                  _ (mapv (partial ensure-datomic! db-uri) [people3 people4 people4 people4])
                  job-id-2 (:job-id (onyx.api/submit-job peer-config job2))]
              (onyx.api/await-job-completion peer-config job-id-2)
              (is (= [{:data '([277076930200560 64 "Mike2" 13194139534319 true]
                               [277076930200561 64 "Dorrene2" 13194139534319 true]
                               [277076930200562 64 "Benti2" 13194139534319 true]), :t 1007}
                      {:data '([277076930200564 64 "Mike3" 13194139534323 true]
                               [277076930200565 64 "Dorrene3" 13194139534323 true]
                               [277076930200566 64 "Benti3" 13194139534323 true]), :t 1011}]
                     (map (fn [result]
                            (-> result
                                (update :data rest)
                                (dissoc :id)))
                          (take-segments! persist 50))))))))
      (finally (d/delete-database db-uri)))))
