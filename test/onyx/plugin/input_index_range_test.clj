(ns onyx.plugin.input-index-range-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.datomic.api :as d]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [read-index-range]]
             [core-async :as core-async]]))

(defn build-job [datomic-config t batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-index-datoms :persist]]
                         :catalog []
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (read-index-range :read-index-datoms
                                    (merge {:datomic/t t
                                            :datomic/index-attribute :user/name
                                            :datomic/index-range-start "Benti"
                                            :datomic/index-range-end "Kristen"
                                            :datomic/datoms-per-segment 20
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
    :db/index true
    :db/cardinality :db.cardinality/one}])

(def people
  [{:user/name "Mike"}
   {:user/name "Dorrene"}
   {:user/name "Benti"}
   {:user/name "Derek"}
   {:user/name "Kristen"}])

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

(defn- type-specific-schema []
  (if (d/client?)
    (mapv #(dissoc % :db/index) schema)
    schema))

(deftest ^:cloud read-index-range-test
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
        _ (mapv (partial ensure-datomic! datomic-config) [[] (type-specific-schema) people])
        t (d/next-t (d/db (d/connect datomic-config)))
        job (build-job datomic-config t 10 1000)
        {:keys [persist]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> job
             (onyx.api/submit-job peer-config)
             :job-id
             (onyx.test-helper/feedback-exception! peer-config))
        (is (= (set (map #(nth % 2) (mapcat :datoms (take-segments! persist 50))))
               #{"Benti" "Derek" "Dorrene"})))
      (finally (d/delete-database datomic-config)))))
