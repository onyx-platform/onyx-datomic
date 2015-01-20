(ns onyx.plugin.input-test
  (:require [midje.sweet :refer :all]
            [datomic.api :as d]
            [onyx.plugin.datomic]
            [onyx.queue.hornetq-utils :as hq-utils]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def scheduler :onyx.job-scheduler/round-robin)

(def env-config
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def peer-config
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id
   :onyx.peer/inbox-capacity 100
   :onyx.peer/outbox-capacity 100
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

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

(def conn (d/connect db-uri))

@(d/transact conn schema)

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

@(d/transact conn people)

(def db (d/db conn))

(def t (d/next-t db))

(def batch-size 1000)

(def hornetq-host "localhost")

(def hornetq-port 5465)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def out-queue (str (java.util.UUID/randomUUID)))

(hq-utils/create-queue! hq-config out-queue)

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

(def workflow {:partition-datoms {:read-datoms {:query :persist}}})

(def catalog
  [{:onyx/name :partition-datoms
    :onyx/ident :datomic/partition-datoms
    :onyx/type :input
    :onyx/medium :datomic
    :onyx/consumption :sequential
    :onyx/bootstrap? true
    :datomic/uri db-uri
    :datomic/t t
    :datomic/datoms-per-segment batch-size
    :datomic/partition :com.mdrogalis/people
    :onyx/batch-size batch-size
    :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms downstream"}

   {:onyx/name :read-datoms
    :onyx/ident :datomic/read-datoms
    :onyx/fn :onyx.plugin.datomic/read-datoms
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :datomic/uri db-uri
    :datomic/t t
    :onyx/doc "Reads and enqueues a range of the :eavt datom index"}

   {:onyx/name :query
    :onyx/fn :onyx.plugin.input-test/my-test-query
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Queries for names of 5 characters or fewer"}

   {:onyx/name :persist
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size
    :onyx/doc "Output source for intermediate query results"}])

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(def results (hq-utils/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(fact (into #{} (mapcat #(apply concat %) (map :names results)))
      => #{"Mike" "Benti" "Derek"})

