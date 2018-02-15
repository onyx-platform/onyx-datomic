(ns onyx.plugin.input-fault-tolerance-test
  "Tests whether the plugin is fault tolerant. Won't make any progress if it restarts each time"
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.datomic.api :as d]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [read-datoms]]
             [core-async :as core-async]]))

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

(def restarted? (atom false))

(def read-datoms-crash
  {:lifecycle/after-batch (fn [event lifecycle]
                            ;; TODO, could do this better with a next epoch lifecycle
                            (when (and (not (empty? (:onyx.core/batch event)))
                                       (not @restarted?)
                                       (zero? (rand-int 10)))
                              (reset! restarted? true)
                              (throw (ex-info "Restartable" {:restartable? true})))
                            {})
   :lifecycle/handle-exception (constantly :restart)})

(def test-state (atom []))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (reset! test-state extent-state))

(defn build-job [datomic-config t batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-datoms :query]
                                    [:query :persist]]
                         :catalog [{:onyx/name :query
                                    :onyx/fn ::my-test-query
                                    :onyx/type :function
                                    :onyx/n-peers 1
                                    :onyx/batch-size batch-size
                                    :onyx/doc "Queries for names of 5 characters or fewer"}]
                         :lifecycles [{:lifecycle/task :read-datoms
                                       :lifecycle/calls ::read-datoms-crash}]
                         :windows [{:window/id :collect-segments
                                    :window/task :persist
                                    :window/type :global
                                    :window/aggregation :onyx.windowing.aggregation/conj}]
                         :triggers [{:trigger/window-id :collect-segments
                                     :trigger/id :trigger-sync
                                     :trigger/refinement :onyx.refinements/accumulating
                                     :trigger/fire-all-extents? true
                                     :trigger/on :onyx.triggers/segment
                                     :trigger/threshold [1 :elements]
                                     :trigger/sync ::update-atom!}]
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (read-datoms :read-datoms
                               (merge {:datomic/t t
                                       :datomic/datoms-index :eavt
                                       :datomic/datoms-per-segment 1
                                       :onyx/max-peers 1}
                                      datomic-config
                                      batch-settings)))
        (add-task (core-async/output :persist (assoc batch-settings :onyx/n-peers 1) 1000000)))))

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
  (mapv (fn [v]
          {;:db/id (d/tempid :com.mdrogalis/people)
           :user/name (str v)})
        (range 10000)))

#_(deftest datomic-input-fault-tolerance-test
    (let [{:keys [env-config peer-config]} (read-config
                                            (clojure.java.io/resource "config.edn")
                                            {:profile :test})
          peer-config (assoc peer-config :onyx.peer/coordinator-barrier-period-ms 50)
          datomic-config (:datomic-config (read-config
                                           (clojure.java.io/resource "config.edn")
                                           {:profile (d/datomic-lib-type)}))
          db-name       (str (java.util.UUID/randomUUID))
          db-uri (str (:datomic/uri datomic-config) db-name)
          datomic-config (assoc datomic-config
                                :datomic/uri db-uri
                                :datomic-cloud/db-name db-name)
          datomic-config (if (string? (:datomic-cloud/proxy-port datomic-config))
                           (assoc datomic-config
                                  :datomic-cloud/proxy-port (Integer/parseInt
                                                             (:datomic-cloud/proxy-port datomic-config)))
                           datomic-config)
          _ (mapv (partial ensure-datomic! datomic-config) [[] schema people])
          t (d/next-t (d/db (d/connect datomic-config)))
          job (build-job datomic-config t 10 1000)
          {:keys [persist]} (get-core-async-channels job)]
      (try
        (with-test-env [test-env [5 env-config peer-config]]
          (->> job
               (onyx.api/submit-job peer-config)
               :job-id
               (onyx.test-helper/feedback-exception! peer-config))

          (is (= (sort (mapcat #(apply concat %) (map :names @test-state)))
                 (sort (map :user/name people)))))
        (finally (d/delete-database datomic-config)))))
