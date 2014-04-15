(ns onyx.plugin.datomic
  (:require [clojure.core.reducers :as r]
            [datomic.api :as d]
            [onyx.peer.pipeline-extensions :as p-ext]))

(defn unroll-datom [db datom]
  [(:e datom)
   (d/ident db (:a datom))
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn datoms-between
  "Returns a reducible collection of datoms created
  between the start and end dates in a single partition."
  [db partition start end]
  (let [start-e (d/entid-at db partition start)
        end-e (d/entid-at db partition end)]
    (->> (d/seek-datoms db :eavt start-e)
         (r/take-while #(< (:e %) end-e)))))

(defmethod p-ext/apply-fn
  {:onyx/type :database
   :onyx/direction :input
   :onyx/medium :datomic}
  [{:keys [task-map] :as pipeline}]
  (let [{:keys [datomic/uri datomic/partition-size datomic/t datomic/partition]} task-map
        conn (d/connect uri)
        partitions (partition-all 2 1 (range 0 t partition-size))]
    {:results (map (fn [[low high]] {:low low :high (or high t) :t t :partition partition})
                   partitions)}))

(defn load-datoms [conn {:keys [low high t partition]}]
  (let [db (d/as-of (d/db conn) t)]
    (->> (datoms-between db partition low high)
         (into [])
         (map (partial unroll-datom db))
         (hash-map :datoms))))

