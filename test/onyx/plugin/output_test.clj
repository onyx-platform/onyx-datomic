(ns onyx.plugin.output-test
  (:require [midje.sweet :refer :all]
            [datomic.api :as d]
            [onyx.plugin.datomic]
            [onyx.queue.hornetq-utils :as hq-utils]
            [onyx.api]))

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

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def in-queue (str (java.util.UUID/randomUUID)))

(def people
  [{:name "Mike"}
   {:name "Dorrene"}
   {:name "Benti"}
   {:name "Kristen"}
   {:name "Derek"}])

(hq-utils/write-and-cap! hq-config in-queue people 1)

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:datomic-uri (str "datomic:mem://" id)
                 :hornetq-host hornetq-host
                 :hornetq-port hornetq-port
                 :zk-addr "127.0.0.1:2181"
                 :onyx-id id
                 :revoke-delay 5000})

(def peer-opts {:hornetq-host hornetq-host
                :hornetq-port hornetq-port
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

(def workflow {:input {:to-datom :output}})

(def catalog
  [{:onyx/name :input
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :hornetq/batch-size 2}

   {:onyx/name :to-datom
    :onyx/fn :onyx.plugin.output-test/to-datom
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 2}
   
   {:onyx/name :output
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

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(doseq [_ (range (count people))]
  (.take tx-queue))

(def results (apply concat (d/q '[:find ?a :where [_ :user/name ?a]] (d/db datomic-conn))))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact (set results) => (set (map :name people)))

