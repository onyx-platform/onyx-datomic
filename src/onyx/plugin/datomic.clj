(ns onyx.plugin.datomic
  (:require [clojure.core.reducers :as r]
            [datomic.api :as d]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :refer [info]]))

(defn unroll-datom
  "Turns a datom into a vector of :eavt+op."
  [db datom]
  [(:e datom)
   (d/ident db (:a datom))
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn datoms-between
  "Returns a reducible collection of datoms created
   between the start and end dates in a single partition.

   https://github.com/candera/strangeloop-2013-datomic/blob/master/slides.org#the-code"
  [db partition start end]
  (let [start-e (d/entid-at db partition start)
        end-e (d/entid-at db partition end)]
    (->> (d/seek-datoms db :eavt start-e)
         (r/take-while #(< (:e %) end-e)))))

(defmethod l-ext/inject-lifecycle-resources
  :datomic/read-datoms
  [_ {:keys [onyx.core/task-map onyx.core/fn-params] :as pipeline}]
  (let [conn (d/connect (:datomic/uri task-map))
        db (d/as-of (d/db conn) (:datomic/t task-map))]
    {:onyx.core/params [db]}))

(defmethod l-ext/inject-lifecycle-resources
  :datomic/commit-tx
  [_ {:keys [onyx.core/task-map]}]
  {:datomic/conn (d/connect (:datomic/uri task-map))})

(defmethod p-ext/apply-fn [:input :datomic]
  [{:keys [onyx.core/task-map] :as pipeline}]
  (let [conn (d/connect (:datomic/uri task-map))
        t (:datomic/t task-map)
        partition (:datomic/partition task-map)
        size (:datomic/datoms-per-segment task-map)
        partitions (partition-all 2 1 (range 0 t size))]
    {:onyx.core/results
     (map (fn [[low high]]
            {:low low :high (or high t) :partition partition})
          partitions)}))

(defn read-datoms [db {:keys [low high partition]}]
  (->> (datoms-between db partition low high)
       (into [])
       (map (partial unroll-datom db))
       (hash-map :datoms)))

(defmethod p-ext/apply-fn [:output :datomic]
  [_] {})

(defmethod p-ext/compress-batch [:output :datomic]
  [{:keys [onyx.core/decompressed] :as pipeline}]
  {:onyx.core/compressed decompressed})

(defmethod p-ext/write-batch [:output :datomic]
  [{:keys [onyx.core/compressed onyx.core/task-map] :as pipeline}]
  @(d/transact (:datomic/conn pipeline)
               (map #(assoc % :db/id (d/tempid (:datomic/partition task-map)))
                    compressed))
  {:onyx.core/written? true})

;; Enable transacting raw batches. It needs the medium to change thanks to the write-batch
;; multimethod dispatching over [:output :datomic-tx]
(defmethod p-ext/apply-fn [:output :datomic-tx] [_] {})

(defmethod p-ext/compress-batch [:output :datomic-tx]
  [{:keys [onyx.core/decompressed] :as pipeline}]
  (if (seq decompressed)
    {:onyx.core/compressed decompressed}
    {}))

(defmethod p-ext/write-batch [:output :datomic-tx]
  [{:keys [onyx.core/compressed onyx.core/task-map] :as pipeline}]
  ;; Transact each tx individually to avoid tempid conflicts.
  (doseq [tx compressed]
    (let [t @(d/transact (:datomic/conn pipeline) (:tx tx))]
      (info t)))
  {:onyx.core/written? (seq compressed)})
