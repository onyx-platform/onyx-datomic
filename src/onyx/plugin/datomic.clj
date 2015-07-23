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
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "Read datoms tasks must set :onyx/max-peers 1" task-map)))
  (let [_ (extensions/write-chunk log :chunk {:chunk-index -1 :status :incomplete} task-id)
        content (extensions/read-chunk log :chunk task-id)]
    (if (= :complete (:status content))
      (throw (Exception. "Restarted task and it was already complete. This is currently unhandled."))
      (let [ch (:read-ch pipeline)
            start-index (:chunk-index content)
            conn (d/connect (:datomic/uri task-map))
            db (d/as-of (d/db conn) (:datomic/t task-map))
            datoms-per-segment (:datomic/datoms-per-segment task-map)
            unroll (partial unroll-datom db)
            num-ignored (* start-index datoms-per-segment)
            datoms-index (:datomic/datoms-index task-map)]
        (go
          (try
            (loop [chunk-index (inc start-index)
                   datoms (seq (drop num-ignored
                                     (d/datoms db datoms-index)))]
              (when datoms 
                (>!! ch (assoc (t/input (java.util.UUID/randomUUID)
                                        {:datoms (map unroll (take datoms-per-segment datoms))})
                               :chunk-index chunk-index))
                (recur (inc chunk-index) 
                       (seq (drop datoms-per-segment datoms)))))
            (>!! ch (t/input (java.util.UUID/randomUUID) :done))
            (catch Exception e
              (fatal e))))
        {:datomic/read-ch ch
         :datomic/drained? (:drained pipeline)
         :datomic/top-chunk-index (:top-chunk-index pipeline)
         :datomic/top-acked-chunk-index (:top-acked-chunk-index pipeline)
         :datomic/pending-chunk-indices (:pending-chunk-indices pipeline) 
         :datomic/pending-messages (:pending-messages pipeline)}))))

(defn highest-acked-chunk [starting-index max-index pending-chunk-indices]
  (loop [max-pending starting-index]
    (if (or (pending-chunk-indices (inc max-pending))
            (= max-index max-pending))
      max-pending
      (recur (inc max-pending)))))

(defrecord DatomicInput [log task-id max-pending batch-size batch-timeout 
                         pending-messages drained?
                         top-chunk-index top-acked-chunk-index pending-chunk-indices 
                         read-ch]
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
                     (keep (fn [_] (first (alts!! [read-ch timeout-ch] :priority true)))))]
      (doseq [m batch]
        (when-let [chunk-index (:chunk-index m)] 
          (swap! top-chunk-index max chunk-index)
          (swap! pending-chunk-indices conj chunk-index))
        (swap! pending-messages assoc (:id m) m))
    (when (and (= 1 (count @pending-messages))
               (= (count batch) 1)
               (= (:message (first batch)) :done))
      (extensions/force-write-chunk log :chunk {:status :complete} task-id)
      (reset! drained? true))
    {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-message [_ _ message-id]
    (let [chunk-index (:chunk-index (@pending-messages message-id))]
      (swap! pending-chunk-indices disj chunk-index)
      (let [new-top-acked (highest-acked-chunk @top-acked-chunk-index @top-chunk-index @pending-chunk-indices)
            updated-content {:chunk-index new-top-acked :status :incomplete}]
        (extensions/force-write-chunk log :chunk updated-content task-id)
        (reset! top-acked-chunk-index new-top-acked))
      (swap! pending-messages dissoc message-id)))

  (retry-message 
    [_ event message-id]
    (when-let [msg (get @pending-messages message-id)]
      (>!! read-ch (assoc msg :id (java.util.UUID/randomUUID))))
    (swap! pending-messages dissoc message-id))

  (pending?
    [_ _ message-id]
    (get @pending-messages message-id))

  (drained? 
    [_ _]
    @drained?))


(defn read-datoms [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:datomic/read-buffer catalog-entry) 1000))] 
    (->DatomicInput (:onyx.core/log pipeline-data)
                    (:onyx.core/task-id pipeline-data)
                    max-pending batch-size batch-timeout 
                    (atom {}) 
                    (atom false) 
                    (atom -1)
                    (atom -1)
                    (atom #{})
                    read-ch)))


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
