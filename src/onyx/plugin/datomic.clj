(ns onyx.plugin.datomic
  (:require [clojure.core.async :refer [chan >! >!! <!! close! thread timeout alts!! go-loop sliding-buffer]]
            [datomic.api :as d]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info debug fatal]]))

(defn safe-connect [task-map]
  (if-let [uri (:datomic/uri task-map)] 
    (d/connect uri)
    (throw (ex-info ":datomic/uri missing from write-datoms task-map." task-map))))

(defn safe-as-of [task-map conn]
  (if-let [t (:datomic/t task-map)]
    (d/as-of (d/db conn) t)
    (throw (ex-info ":datomic/t missing from write-datoms task-map." task-map))))

(defn safe-datoms-per-segment [task-map]
  (or (:datomic/datoms-per-segment task-map)
      (throw (ex-info ":datomic/datoms-per-segment missing from write-datoms task-map." task-map))))

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; input plugins

(defn unroll-datom
  "Turns a datom into a vector of :eavt+op."
  [db datom]
  [(:e datom)
   (d/ident db (:a datom))
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn datoms-sequence [db task-map]
  (case (:onyx/plugin task-map)
    ::read-datoms
    (let [datoms-components (or (:datomic/datoms-components task-map) [])
          datoms-index (:datomic/datoms-index task-map)]
      (apply d/datoms db datoms-index datoms-components))
    ::read-index-range
    (let [attribute (:datomic/index-attribute task-map)
          range-start (:datomic/index-range-start task-map)
          range-end (:datomic/index-range-end task-map)]
      (d/index-range db attribute range-start range-end))))

(defn start-commit-loop! [commit-ch log task-id]
  (go-loop []
           (when-let [content (<!! commit-ch)] 
             (extensions/force-write-chunk log :chunk content task-id)
             (recur))))

(defn close-read-datoms-resources 
  [{:keys [datomic/producer-ch datomic/commit-ch datomic/read-ch] :as event} lifecycle]
  (close! read-ch)
  (close! commit-ch)
  (close! producer-ch))

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
            conn (safe-connect task-map)
            db (safe-as-of task-map conn)
            datoms-per-segment (safe-datoms-per-segment task-map)
            unroll (partial unroll-datom db)
            num-ignored (* start-index datoms-per-segment)
            commit-loop-ch (start-commit-loop! (:commit-ch pipeline) log task-id)
            producer-ch (thread
                          (try
                            (loop [chunk-index (inc start-index)
                                   datoms (seq (drop num-ignored
                                                     (datoms-sequence db task-map)))]
                              (when datoms 
                                (let [success? (>!! ch (assoc (t/input (java.util.UUID/randomUUID)
                                                                       {:datoms (map unroll (take datoms-per-segment datoms))})
                                                              :chunk-index chunk-index))] 
                                  (if success?
                                    (recur (inc chunk-index) 
                                           (seq (drop datoms-per-segment datoms)))))))
                            (>!! ch (t/input (java.util.UUID/randomUUID) :done))
                            (catch Exception e
                              (fatal e))))]
        
        {:datomic/read-ch ch
         :datomic/commit-ch (:commit-ch pipeline)
         :datomic/producer-ch producer-ch
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
                         read-ch commit-ch]
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
      (>!! commit-ch {:status :complete}) 
      (reset! drained? true))
    {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-segment [_ _ segment-id]
    (let [chunk-index (:chunk-index (@pending-messages segment-id))]
      (swap! pending-chunk-indices disj chunk-index)
      (let [new-top-acked (highest-acked-chunk @top-acked-chunk-index @top-chunk-index @pending-chunk-indices)]
        (>!! commit-ch {:chunk-index new-top-acked :status :incomplete})
        (reset! top-acked-chunk-index new-top-acked))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment 
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (>!! read-ch (assoc msg :id (java.util.UUID/randomUUID))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained? 
    [_ _]
    @drained?))

(defn shared-input-builder [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:datomic/read-buffer catalog-entry) 1000))
        commit-ch (chan (sliding-buffer 1))] 
    (->DatomicInput (:onyx.core/log pipeline-data)
                    (:onyx.core/task-id pipeline-data)
                    max-pending batch-size batch-timeout 
                    (atom {}) 
                    (atom false) 
                    (atom -1)
                    (atom -1)
                    (atom #{})
                    read-ch
                    commit-ch)))

(defn read-datoms [pipeline-data]
  (shared-input-builder pipeline-data))

(defn read-index-range [pipeline-data]
  (shared-input-builder pipeline-data))

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; read log plugin

(defn unroll-log-datom
  "Turns a log datom into a vector of :eavt+op."
  [datom]
  [(:e datom)
   (:a datom)
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn close-read-log-resources 
  [{:keys [datomic/producer-ch datomic/commit-ch datomic/read-ch] :as event} lifecycle]
  (close! read-ch)
  (close! commit-ch)
  (close! producer-ch))

(defn inject-read-log-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "Read log tasks must set :onyx/max-peers 1" task-map)))
  (let [start-tx (:datomic/log-start-tx task-map)
        max-tx (:datomic/log-end-tx task-map)
        read-size (or (:datomic/read-max-chunk-size task-map) 1000)
        ch (:read-ch pipeline)
        conn (safe-connect task-map)
        producer-ch (thread
                      (try
                        (loop [tx-index start-tx]
                          ;; tx-range called up to maximum tx in each iteration
                          ;; relies on the fact that tx-range is lazy, therefore only read-size elements will be realised
                          ;; use a nil end-tx, and rely on removing higher tx values
                          (if-let [entries (seq 
                                             (take read-size 
                                                   (seq 
                                                     (d/tx-range (d/log conn) tx-index nil))))]
                            (let [last-t (:t (last entries))
                                  next-t (inc last-t)] 
                              (doseq [entry (filter #(or (nil? max-tx)  
                                                         (< (:t %) max-tx)) 
                                                    entries)]
                                (>!! ch 
                                     (t/input (java.util.UUID/randomUUID)
                                              (update (into {} entry)
                                                      :data (partial map unroll-log-datom)))))


                              (if (or (nil? max-tx) 
                                      (< last-t max-tx))
                                (recur next-t)))
                            ;; timeout could be used to backoff here when no entries are read
                            (recur tx-index)))
                        (>!! ch (t/input (java.util.UUID/randomUUID) :done))
                        (catch Exception e
                          (fatal e))))]

    {:datomic/read-ch ch
     :datomic/commit-ch (:commit-ch pipeline)
     :datomic/producer-ch producer-ch
     :datomic/drained? (:drained pipeline)
     :datomic/pending-messages (:pending-messages pipeline)}))

(defrecord DatomicLogInput 
  [log task-id max-pending batch-size batch-timeout pending-messages drained? read-ch commit-ch]
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
        (swap! pending-messages assoc (:id m) m))
      (when (and (= 1 (count @pending-messages))
                 (= (count batch) 1)
                 (= (:message (first batch)) :done))
        (reset! drained? true))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-segment [_ _ segment-id]
    (swap! pending-messages dissoc segment-id))

  (retry-segment 
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (>!! read-ch (assoc msg :id (java.util.UUID/randomUUID))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained? 
    [_ _]
    @drained?))

(defn read-log [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:datomic/read-buffer catalog-entry) 1000))
        commit-ch (chan (sliding-buffer 1))] 
    (->DatomicLogInput (:onyx.core/log pipeline-data)
                       (:onyx.core/task-id pipeline-data)
                       max-pending batch-size batch-timeout 
                       (atom {}) 
                       (atom false) 
                       read-ch
                       commit-ch)))

(def read-log-calls
  {:lifecycle/before-task-start inject-read-log-resources
   :lifecycle/after-task-stop close-read-log-resources})

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; output plugins

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
                   (map (fn [msg] (if (and partition (not (sequential? msg))) 
                                    (assoc msg :db/id (d/tempid partition))
                                    msg))
                        (map :message messages)))
      {:onyx.core/written? true}))

  (seal-resource 
    [_ _]
    {}))

(defn write-datoms [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        conn (safe-connect task-map) 
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
        conn (safe-connect task-map)] 
    (->DatomicWriteBulkDatoms conn)))

(def read-datoms-calls
  {:lifecycle/before-task-start inject-read-datoms-resources
   :lifecycle/after-task-stop close-read-datoms-resources})

(def read-index-range-calls
  {:lifecycle/before-task-start inject-read-datoms-resources
   :lifecycle/after-task-stop close-read-datoms-resources})


(def write-tx-calls
  {:lifecycle/before-task-start inject-write-tx-resources})

(def write-bulk-tx-calls
  {:lifecycle/before-task-start inject-write-bulk-tx-resources})


;;;;;;;;;
;;; params lifecycles

(defn inject-db [{:keys [onyx.core/params] :as event} {:keys [datomic/basis-t datomic/uri onyx/param?] :as lifecycle}]
  (when-not uri
    (throw (ex-info "Missing :datomic/uri in inject-db-calls lifecycle." lifecycle)))
  (let [conn (d/connect (:datomic/uri lifecycle))
        db (cond-> (d/db conn)
             basis-t (d/as-of basis-t))]
    {:datomic/conn conn
     :datomic/db db
     :onyx.core/params (if param? 
                         (conj params db)
                         params)}))

(def inject-db-calls
  {:lifecycle/before-task-start inject-db})

(defn inject-conn [{:keys [onyx.core/params] :as event} {:keys [datomic/uri onyx/param?] :as lifecycle}]
  (when-not uri
    (throw (ex-info "Missing :datomic/uri in inject-conn-calls lifecycle."
                    lifecycle)))
  (let [conn (d/connect uri)] 
    {:datomic/conn conn
     :onyx.core/params (if param? 
                         (conj params conn)
                         params)}))

(def inject-conn-calls
  {:lifecycle/before-task-start inject-conn})
