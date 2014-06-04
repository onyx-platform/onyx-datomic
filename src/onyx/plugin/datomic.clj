(ns onyx.plugin.datomic
  (:require [clojure.core.reducers :as r]
            [datomic.api :as d]
            [onyx.peer.task-lifecycle-extensions :as l-ext]))

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
  :datomic/load-datoms
  [_ {:keys [task-map fn-params] :as pipeline}]
  (let [conn (d/connect (:datomic/uri task-map))
        db (d/as-of (d/db conn) (:datomic/t task-map))]
    {:params [db]}))

(defmethod l-ext/inject-lifecycle-resources
  :datomic/commit-tx
  [_ {:keys [task-map]}]
  {:datomic/conn (d/connect (:datomic/uri task-map))})

(defmethod l-ext/apply-fn [:input :datomic]
  [{:keys [task-map] :as pipeline}]
  (let [conn (d/connect (:datomic/uri task-map))
        t (:datomic/t task-map)
        partition (:datomic/partition task-map)
        size (:datomic/datoms-per-segment task-map)
        partitions (partition-all 2 1 (range 0 t size))]
    {:results (map (fn [[low high]]
                     {:low low :high (or high t) :partition partition})
                   partitions)}))

(defn load-datoms [db {:keys [low high partition]}]
  (->> (datoms-between db partition low high)
       (into [])
       (map (partial unroll-datom db))
       (hash-map :datoms)))

(defmethod l-ext/apply-fn [:output :datomic]
  [_]
  {})

(defmethod l-ext/compress-batch [:output :datomic]
  [{:keys [decompressed] :as pipeline}]
  {:compressed decompressed})

(defmethod l-ext/write-batch [:output :datomic]
  [{:keys [compressed] :as pipeline}]
  @(d/transact (:datomic/conn pipeline) (mapcat :datoms compressed))
  {:written? true})

