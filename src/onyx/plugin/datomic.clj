(ns onyx.plugin.datomic
  (:require [clojure.core.async :refer [chan >! >!! <!! close! go timeout alts!!]]
            [datomic.api :as d]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info fatal]]))

(defn unroll-datom
  "Turns a datom into a vector of :eavt+op."
  [db datom]
  [(:e datom)
   (d/ident db (:a datom))
   (:v datom)
   (:tx datom)
   (:added datom)])

(defmethod l-ext/inject-lifecycle-resources :datomic/read-datoms
  [_ {:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as event}]
  (let [ch (chan (or (:datomic/read-buffer task-map) 1000))
        conn (d/connect (:datomic/uri task-map))
        db (d/as-of (d/db conn) (:datomic/t task-map))]
    (go
     (try
       (let [iterator (d/datoms db (:datomic/datoms-index task-map))]
         (doseq [datoms (partition-all (:onyx/batch-size task-map) iterator)]
           (>!! ch {:content (map unroll-datom datoms)}))
         (>!! ch :done))
       (catch Exception e
         (fatal e))))
    {:datomic/read-ch ch
     :datomi/pending-messages (atom {})}))

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

(defmethod l-ext/inject-lifecycle-resources :datomic/commit-tx
  [_ {:keys [onyx.core/task-map]}]
  {:datomic/conn (d/connect (:datomic/uri task-map))})

(defmethod p-ext/write-batch [:output :datomic]
  [{:keys [onyx.core/compressed onyx.core/task-map] :as pipeline}]
  @(d/transact (:datomic/conn pipeline)
               (map #(assoc % :db/id (d/tempid (:datomic/partition task-map)))
                    compressed))
  {:onyx.core/written? true})

(defmethod p-ext/write-batch [:output :datomic-tx]
  [{:keys [onyx.core/compressed onyx.core/task-map] :as pipeline}]
  ;; Transact each tx individually to avoid tempid conflicts.
  (doseq [tx compressed]
    (let [t @(d/transact (:datomic/conn pipeline) (:tx tx))]
      (info t)))
  {:onyx.core/written? (seq compressed)})
