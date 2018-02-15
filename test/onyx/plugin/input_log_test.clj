(ns onyx.plugin.input-log-test
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
             [core-async :as core-async]]
            [taoensso.timbre :refer [info spy]]))

(defn build-job [datomic-config log-start-tx log-end-tx batch-size batch-timeout]
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
                                    :onyx/max-peers 1
                                    :datomic/log-start-tx log-start-tx
                                    :datomic/log-end-tx log-end-tx}
                                   datomic-config
                                   batch-settings)))
        (add-task (core-async/output :persist batch-settings 100000)))))

(defn ensure-datomic!
  [datomic-config data]
  (d/create-database datomic-config)
  (d/transact
   (d/connect datomic-config)
   data))

(def schema
  [{:db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

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

(def people3
  [{:user/name "Mike3"}
   {:user/name "Dorrene3"}
   {:user/name "Benti3"}])

(def people4
  [{:user/name "Mike4"}
   {:user/name "Dorrene4"}
   {:user/name "Benti4"}])

(deftest ^:cloud datomic-input-log-test
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
                         datomic-config)]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (testing "That we can read the initial transaction log"
          (let [log-start-tx-1 (if (d/client?) 5 1000)
                log-end-tx-1 (if (d/client?) 5 1001)
                log-start-tx-2 (if (d/client?) 6 1001)
                log-end-tx-2 (if (d/client?) 7 1014)
                job (build-job datomic-config log-start-tx-1 log-end-tx-1 10 1000)
                {:keys [persist]} (get-core-async-channels job)
                _ (mapv (partial ensure-datomic! datomic-config) [schema people])
                job-id (:job-id (onyx.api/submit-job peer-config job))]
            (ensure-datomic! datomic-config people2)
            (onyx.api/await-job-completion peer-config job-id)
            (is (= (if (d/peer?)
                     [{:data '([63 10 :com.mdrogalis/people 13194139534312 true]
                               [0 11 63 13194139534312 true]
                               [64 10 :user/name 13194139534312 true]
                               [64 40 23 13194139534312 true]
                               [64 41 35 13194139534312 true]
                               [0 13 64 13194139534312 true]) :t 1000}
                      {:data '([17592186045418 64 "Mike" 13194139534313 true]
                               [17592186045419 64 "Dorrene" 13194139534313 true]
                               [17592186045420 64 "Benti" 13194139534313 true]
                               [17592186045421 64 "Derek" 13194139534313 true]
                               [17592186045422 64 "Kristen" 13194139534313 true]) :t 1001}]
                     [{:t 5
                       :tx-data '([65 "Mike" 13194139533317 true]
                                  [65 "Dorrene" 13194139533317 true]
                                  [65 "Benti" 13194139533317 true]
                                  [65 "Derek" 13194139533317 true]
                                  [65 "Kristen" 13194139533317 true])}])
                   (map (fn [result]
                          (let [r (-> result
                                      (update (if (d/peer?) :data :tx-data) rest)
                                      (dissoc :id))]
                            (if (d/client?)
                              (assoc r :tx-data (mapv rest (:tx-data r)))
                              r)))
                        (take-segments! persist 50))))
            ;; recover from resume point, should only get the latest txes
            (let [new-job (build-job datomic-config log-start-tx-2 log-end-tx-2 10 1000)
                  job2 (->> job-id
                            (onyx.api/job-snapshot-coordinates peer-config (:onyx/tenancy-id peer-config))
                            (onyx.api/build-resume-point new-job)
                            (assoc new-job :resume-point))
                  {:keys [persist]} (get-core-async-channels job2)
                  _ (mapv (partial ensure-datomic! datomic-config) [people3 people4 people4 people4])
                  job-id-2 (:job-id (onyx.api/submit-job peer-config job2))]
              (onyx.api/await-job-completion peer-config job-id-2)
              (is (= (if (d/peer?)
                       [{:data '([17592186045424 64 "Mike2" 13194139534319 true]
                                 [17592186045425 64 "Dorrene2" 13194139534319 true]
                                 [17592186045426 64 "Benti2" 13194139534319 true]), :t 1007}
                        {:data '([17592186045428 64 "Mike3" 13194139534323 true]
                                 [17592186045429 64 "Dorrene3" 13194139534323 true]
                                 [17592186045430 64 "Benti3" 13194139534323 true]), :t 1011}]
                       [{:t 6
                         :tx-data '([65 "Mike2" 13194139533318 true]
                                    [65 "Dorrene2" 13194139533318 true]
                                    [65 "Benti2" 13194139533318 true])}
                        {:t 7
                         :tx-data '([65 "Mike3" 13194139533319 true]
                                    [65 "Dorrene3" 13194139533319 true]
                                    [65 "Benti3" 13194139533319 true])}])
                     (map (fn [result]
                            (let [r (-> result
                                        (update (if (d/peer?) :data :tx-data) rest)
                                        (dissoc :id))]
                              (if (d/client?)
                                (assoc r :tx-data (mapv rest (:tx-data r)))
                                r)))
                          (take-segments! persist 50))))))))
      (finally (d/delete-database datomic-config)))))

{:t 5, :tx-data '([13194139533317 50 #inst "2018-02-10T04:49:38.947-00:00" 13194139533317 true] [68372031061622850 65 "Mike" 13194139533317 true] [17381079811883075 65 "Dorrene" 13194139533317 true] [7916483719987268 65 "Benti" 13194139533317 true] [37488948460650565 65 "Derek" 13194139533317 true] [2942293115928646 65 "Kristen" 13194139533317 true])}
