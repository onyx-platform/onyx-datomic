(ns onyx.plugin.output-test
  (:require [midje.sweet :refer :all]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.plugin.datomic]
            [onyx.queue.hornetq-utils :as hq-utils]
            [onyx.system :refer [onyx-development-env]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id})

(def peer-config
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id
   :onyx.peer/inbox-capacity 100
   :onyx.peer/outbox-capacity 100
   :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin})

(def dev (onyx-development-env env-config))

(def env (component/start dev))

(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

(def input-queue-name (str (java.util.UUID/randomUUID)))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(d/create-database db-uri)

(def datomic-conn (d/connect db-uri))

@(d/transact datomic-conn schema)

(def tx-queue (d/tx-report-queue datomic-conn))

(def hornetq-host "localhost")

(def hornetq-port 5465)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def in-queue (str (java.util.UUID/randomUUID)))

(hq-utils/create-queue! hq-config in-queue)

(def people
  [{:name "Mike"}
   {:name "Dorrene"}
   {:name "Benti"}
   {:name "Kristen"}
   {:name "Derek"}])

(hq-utils/write-and-cap! hq-config in-queue people 1)

(def workflow {:in {:to-datom :out}})

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 2}

   {:onyx/name :to-datom
    :onyx/fn :onyx.plugin.output-test/to-datom
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size 2}
   
   {:onyx/name :out
    :onyx/ident :datomic/commit-tx
    :onyx/type :output
    :onyx/medium :datomic
    :onyx/consumption :concurrent
    :datomic/uri db-uri
    :onyx/batch-size 2
    :onyx/doc "Transacts :datoms to storage"}])

(defn to-datom [{:keys [name] :as segment}]
  {:datoms [{:db/id (d/tempid :com.mdrogalis/people)
             :user/name name}]})

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(doseq [_ (range (count people))]
  (.take tx-queue))

(def results (apply concat (d/q '[:find ?a :where [_ :user/name ?a]] (d/db datomic-conn))))

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(component/stop env)

(fact (set results) => (set (map :name people)))

