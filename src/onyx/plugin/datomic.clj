(ns onyx.plugin.datomic
  (:require [clojure.core.async :refer [chan >! >!! <!! close! go timeout alts!!]]
            [datomic.api :as d]
            [onyx.peer.pipeline-extensions :as p-ext]
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
  (let [ch (chan (or (:datomic/read-buffer task-map) 1000))
        conn (d/connect (:datomic/uri task-map))
        db (d/as-of (d/db conn) (:datomic/t task-map))]
    (go
     (try
       (let [d-seq (d/datoms db (:datomic/datoms-index task-map))]
         (doseq [datoms (partition-all (:datomic/datoms-per-segment task-map) d-seq)]
           (>!! ch {:content {:datoms (map (partial unroll-datom db) datoms)}})))
       (>!! ch :done)
       (catch Exception e
         (fatal e))))
    {:datomic/read-ch ch
     :datomic/pending-messages (atom {})}))

(defmethod p-ext/read-batch :datomic/read-datoms
  [{:keys [datomic/read-ch datomic/pending-messages onyx.core/task-map]}]
  (let [pending (count (keys @pending-messages))
        max-pending (or (:onyx/max-pending task-map) 10000)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) 50)
        timeout-ch (timeout ms)
        batch (->> (range max-segments)
                   (map (fn [_]
                          (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                            (if (= :done result)
                              {:id (java.util.UUID/randomUUID)
                               :input :datomic
                               :message :done}
                              {:id (java.util.UUID/randomUUID)
                               :input :datomic
                               :message (:content result)}))))
                   (remove (comp nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (select-keys m [:message])))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :datomic/read-datoms
  [{:keys [datomic/pending-messages onyx.core/log onyx.core/task-id]} message-id]
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message :datomic/read-datoms
  [{:keys [datomic/pending-messages datomic/read-ch onyx.core/log]} message-id]
  (let [msg (get @pending-messages message-id)]
    (if (= :done (:message msg))
      (>!! read-ch :done)
      (>!! read-ch (get @pending-messages message-id))))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/pending? :datomic/read-datoms
  [{:keys [datomic/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :datomic/read-datoms
  [{:keys [datomic/pending-messages]}]
  (let [x @pending-messages]
    (and (= (count (keys x)) 1)
         (= (first (map :message (vals x))) :done))))

(defn inject-write-tx-resources
  [{:keys [onyx.core/task-map]} lifecycle]
  {:datomic/conn (d/connect (:datomic/uri task-map))})

(defn inject-write-bulk-tx-resources
  [{:keys [onyx.core/task-map]} lifecycle]
  {:datomic/conn (d/connect (:datomic/uri task-map))})

(defmethod p-ext/write-batch :datomic/commit-tx
  [{:keys [onyx.core/results onyx.core/task-map] :as pipeline}]
  (let [messages (mapcat :leaves results)]
    @(d/transact (:datomic/conn pipeline)
                 (map #(assoc % :db/id (d/tempid (:datomic/partition task-map)))
                      (map :message messages)))
    {:onyx.core/written? true}))

(defmethod p-ext/write-batch :datomic/commit-bulk-tx
  [{:keys [onyx.core/results] :as pipeline}]
  ;; Transact each tx individually to avoid tempid conflicts.
  (doseq [tx (mapcat :leaves results)]
    @(d/transact (:datomic/conn pipeline) (:tx (:message tx))))
  {:onyx.core/written? true})

(defmethod p-ext/seal-resource :datomic/commit-tx
  [event]
  {})

(defmethod p-ext/seal-resource :datomic/commit-bulk-tx
  [event]
  {})

(def read-datoms-calls
  {:lifecycle/before-task-start inject-read-datoms-resources})

(def write-tx-calls
  {:lifecycle/before-task-start inject-write-tx-resources})

(def write-bulk-tx-calls
  {:lifecycle/before-task-start inject-write-bulk-tx-resources})
