(ns onyx.plugin.datomic-test
  (:require [clojure.data.fressian :as fressian]
            [midje.sweet :refer :all]
            [datomic.api :as d]
            [onyx.plugin.datomic]
            [onyx.api])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defn create-queue [session queue-name]
  (try
    (.createQueue session queue-name queue-name true)
    (catch Exception e)))

(defn consume-queue! [config queue-name echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]

    (create-queue session queue-name)

    (let [consumer (.createConsumer session queue-name)
          results (atom [])]
      (.start session)
      (while (not= (last @results) :done)
        (when (zero? (mod (count @results) echo))
          (prn (format "[HQ Util] Read %s segments" (count @results))))
        (let [message (.receive consumer)]
          (when message
            (.acknowledge message)
            (swap! results conj (fressian/read (.toByteBuffer (.getBodyBuffer message)))))))

      (prn "[HQ Util] Done reading")
      (.commit session)
      (.close consumer)
      (.close session)

      @results)))

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

(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def out-queue (str (java.util.UUID/randomUUID)))

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
                :onyx-id id
                :fn-params {:load-datoms [conn]}})

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  (prn "--> " segment)
  {:names (d/q query datoms)})

(def workflow {:partition-datoms {:load-datoms {:datomic-query :out}}})

(def catalog
  [{:onyx/name :partition-datoms
    :onyx/direction :input
    :onyx/type :database
    :onyx/medium :datomic
    :onyx/consumption :sequential
    :onyx/bootstrap? true
    :datomic/uri db-uri
    :datomic/t t
    :datomic/partition-size 1000
    :datomic/partition :com.mdrogalis/people
    :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms"}

   {:onyx/name :load-datoms
    :onyx/fn :onyx.plugin.datomic/load-datoms
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 2
    :onyx/doc "Reads and enqueues a range of the :eavt datom index"}

   {:onyx/name :datomic-query
    :onyx/fn :onyx.plugin.datomic-test/my-test-query
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 2
    :onyx/doc "Queries for names of 5 characters or fewer"}

   {:onyx/name :out
    :onyx/direction :output
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 2
    :onyx/doc "Output source for intermediate query results"}])

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact (into #{} (mapcat #(apply concat %) (map :names results)))
      => #{"Mike" "Benti" "Derek"})

