(ns onyx.plugin.tx-output-test
  (:require [midje.sweet :refer :all]
            [datomic.api :as d]
            [onyx.plugin.datomic]
            [onyx.queue.hornetq-utils :as hq-utils]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def scheduler :onyx.job-scheduler/round-robin)

(def env-config
  {:hornetq/mode :standalone
   :hornetq/server? true
   :hornetq.server/type :embedded
   :hornetq.embedded/config ["hornetq/non-clustered-1.xml"]
   :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
   :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def peer-config
  {:hornetq/mode :standalone
   :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
   :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

(def input-queue-name (str (java.util.UUID/randomUUID)))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :name
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   
   {:db/id #db/id [:db.part/db]
    :db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(d/create-database db-uri)

(def datomic-conn (d/connect db-uri))

(def tx-queue (d/tx-report-queue datomic-conn))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def in-queue (str (java.util.UUID/randomUUID)))

(hq-utils/create-queue! hq-config in-queue)

(def txes
  [schema
   (map #(assoc % :db/id (d/tempid :db.part/user))
        [{:name "Mike" :age 27}
         {:name "Dorrene" :age 21}
         {:name "Benti" :age 10}
         {:name "Kristen"}
         {:name "Derek"}])
   [{:db/id [:name "Mike"] :age 30}]
   [[:db/retract [:name "Dorrene"] :age 21]]
   [[:db.fn/cas [:name "Benti"] :age 10 18]]])

(hq-utils/write-and-cap! hq-config in-queue txes 1)

(def workflow {:in {:identity :out}})

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size 2}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size 2}
   
   {:onyx/name :out
    :onyx/ident :datomic/commit-tx
    :onyx/type :output
    :onyx/medium :datomic-tx
    :onyx/consumption :concurrent
    :datomic/uri db-uri
    :datomic/partition :com.mdrogalis/people
    :onyx/batch-size 2
    :onyx/doc "Transacts segments to storage"}])

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(doseq [_ (range (count txes))]
  (.take tx-queue))

(let [db (d/db datomic-conn)]
  (def results
    (map (comp (juxt :name :age) (partial d/entity db)) (apply concat (d/q '[:find ?e :where [?e :name]] db)))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(fact (set results) => #{["Mike" 30] ["Dorrene" nil] ["Benti" 18] ["Kristen" nil] ["Derek" nil]})

