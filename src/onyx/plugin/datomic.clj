(ns onyx.plugin.datomic
  (:require [clojure.core.async :refer [chan >! >!! <!! close! go timeout alts!!]]
            [datomic.api :as d]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info debug fatal]]))

(defn unroll-datom
  "Turns a datom into a vector of :eavt+op."
  [db datom]
  [(:e datom)
   (d/ident db (:a datom))
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn inject-read-datoms-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "Read datoms tasks must set :onyx/max-peers 1" task-map)))
  (let [ch (:read-ch (:onyx.core/pipeline event))
        conn (d/connect (:datomic/uri task-map))
        db (d/as-of (d/db conn) (:datomic/t task-map))
        datoms-index (:datomic/datoms-index task-map)
        datoms-per-segment (:datomic/datoms-per-segment task-map)]
    (go
     (try
       (let [d-seq (d/datoms db datoms-index)]
         (doseq [datoms (partition-all datoms-per-segment d-seq)]
           (>!! ch {:datoms (map (partial unroll-datom db) datoms)})))
       (>!! ch :done)
       (catch Exception e
         (fatal e))))
    {}))

(defrecord DatomicInput [max-pending batch-size batch-timeout 
                         pending-messages drained read-ch retry-ch]
  p-ext/Pipeline
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch 
    [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (->> (range max-segments)
                     (map (fn [_]
                            (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                              (if (= :done result)
                                (t/input (java.util.UUID/randomUUID) :done)
                                (t/input (java.util.UUID/randomUUID) result)))))
                     (filter :message))]
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) (:message m)))
      (when (and (= 1 (count @pending-messages))
                 (= (count batch) 1)
                 (= (:message (first batch)) :done))
        (reset! drained true))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-message [_ _ message-id]
    (swap! pending-messages dissoc message-id))

  (retry-message 
    [_ event message-id]
    (when-let [msg (get @pending-messages message-id)]
      (if (= :done msg)
        (>!! read-ch :done)
        (>!! read-ch msg)))
    (swap! pending-messages dissoc message-id))

  (pending?
    [_ _ message-id]
    (get @pending-messages message-id))

  (drained? 
    [_ _]
    @drained))

(defn read-datoms [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:datomic/read-buffer catalog-entry) 1000))] 
    (->DatomicInput max-pending batch-size batch-timeout 
                    (atom {}) (atom false) read-ch (chan 10000))))


(defn inject-write-tx-resources
  [{:keys [onyx.core/pipeline]} lifecycle]
  {:datomic/conn (:conn pipeline)})

(defn inject-write-bulk-tx-resources
  [{:keys [onyx.core/pipeline]} lifecycle]
  {:datomic/conn (:conn pipeline)})

(defrecord DatomicWriteDatoms [conn partition]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    [_ event]
    (let [messages (mapcat :leaves (:tree (:onyx.core/results event)))]
      @(d/transact conn
                   (map #(assoc % :db/id (d/tempid partition))
                        (map :message messages)))
      {:onyx.core/written? true}))

  (seal-resource 
    [_ _]
    {}))

(defn write-datoms [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        conn (d/connect (:datomic/uri task-map))
        partition (:datomic/partition task-map)] 
    (->DatomicWriteDatoms conn partition)))

(defrecord DatomicWriteBulkDatoms [conn]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    [_ event]
    ;; Transact each tx individually to avoid tempid conflicts.
    (doseq [tx (mapcat :leaves (:tree (:onyx.core/results event)))]
      @(d/transact conn (:tx (:message tx))))
    {:onyx.core/written? true})

  (seal-resource 
    [_ _]
    {}))

(defn write-bulk-datoms [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        conn (d/connect (:datomic/uri task-map))] 
    (->DatomicWriteBulkDatoms conn)))

(def read-datoms-calls
  {:lifecycle/before-task-start inject-read-datoms-resources})

(def write-tx-calls
  {:lifecycle/before-task-start inject-write-tx-resources})

(def write-bulk-tx-calls
  {:lifecycle/before-task-start inject-write-bulk-tx-resources})
