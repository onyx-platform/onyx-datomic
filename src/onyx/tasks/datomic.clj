(ns onyx.tasks.datomic
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def DatomicReadLogTaskMap
  {:datomic/uri s/Str 
   :checkpoint/force-reset? s/Bool
   (s/optional-key :datomic/log-start-tx) s/Int
   (s/optional-key :datomic/log-end-tx) s/Int
   (s/optional-key :checkpoint/key) s/Str
   (s/optional-key :onyx/max-peers) (s/enum 1)
   (s/optional-key :onyx/n-peers) (s/enum 1)
   (os/restricted-ns :datomic) s/Any})

(s/defn ^:always-validate read-log
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.datomic/read-log
                             :onyx/type :input
                             :onyx/medium :datomic}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.datomic/read-log-calls}]}
    :schema {:task-map DatomicReadLogTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    force-reset? :- s/Bool
    task-opts :- {s/Any s/Any}]
   (read-log task-name (merge {:datomic/uri uri
                               :checkpoint/force-reset? force-reset?}
                              task-opts))))

(def DatomicReadDatomsTaskMap
  {:datomic/uri s/Str
   :datomic/t s/Int
   :datomic/datoms-index s/Keyword
   :datomic/datoms-per-segment s/Int
   (s/optional-key :datomic/datoms-components) [s/Any]
   (s/optional-key :onyx/max-peers) (s/enum 1)
   (s/optional-key :onyx/n-peers) (s/enum 1)
   (os/restricted-ns :datomic) s/Any})

(s/defn ^:always-validate read-datoms
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.datomic/read-datoms
                             :onyx/type :input
                             :onyx/medium :datomic}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.datomic/read-datoms-calls}]}
    :schema {:task-map DatomicReadDatomsTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    t :- s/Int
    datoms-index :- s/Keyword
    datoms-per-segment :- s/Int
    task-opts :- {s/Any s/Any}]
   (read-datoms task-name
                (merge {:datomic/uri uri
                        :datomic/t t
                        :datomic/datoms-index datoms-index
                        :datomic/datoms-per-segment datoms-per-segment}
                       task-opts))))

(def DatomicReadIndexRangeTaskMap
  {:datomic/uri s/Str
   :datomic/t s/Int
   :datomic/index-attribute s/Any
   :datomic/index-range-start s/Any
   :datomic/index-range-end s/Any
   :datomic/datoms-per-segment s/Int
   (s/optional-key :onyx/max-peers) (s/enum 1)
   (s/optional-key :onyx/n-peers) (s/enum 1)
   (os/restricted-ns :datomic) s/Any})

(s/defn ^:always-validate read-index-range
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.datomic/read-index-range
                             :onyx/type :input
                             :onyx/medium :datomic}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.datomic/read-index-range-calls}]}
    :schema {:task-map DatomicReadIndexRangeTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    t :- s/Int
    index-attribute :- s/Any
    index-range-start :- s/Any
    index-range-end :- s/Any
    datoms-per-segment :- s/Int
    task-opts :- {s/Any s/Any}]
   (read-index-range task-name (merge {:datomic/uri uri
                                       :datomic/t t
                                       :datomic/index-attribute index-attribute
                                       :datomic/index-range-start index-range-start
                                       :datomic/index-range-end index-range-end
                                       :datomic/datoms-per-segment datoms-per-segment}
                                      task-opts))))

(def DatomicWriteDatomsTaskMap
  {:datomic/uri s/Str
   (s/optional-key :datomic/partition) (s/either s/Int s/Keyword)
   (s/optional-key :onyx/max-peers) (s/enum 1)
   (s/optional-key :onyx/n-peers) (s/enum 1)
   (os/restricted-ns :datomic) s/Any})

(s/defn ^:always-validate write-bulk-tx-datoms
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms
                             :onyx/type :output
                             :onyx/medium :datomic}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-calls}]}
    :schema {:task-map DatomicWriteDatomsTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    task-opts :- {s/Any s/Any}]
   (write-bulk-tx-datoms task-name (merge {:datomic/uri uri}
                                          task-opts))))

(s/defn ^:always-validate write-datoms
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.datomic/write-datoms
                             :onyx/type :output
                             :onyx/medium :datomic}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.datomic/write-tx-calls}]}
    :schema {:task-map DatomicWriteDatomsTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    task-opts :- {s/Any s/Any}]
   (write-datoms task-name (merge {:datomic/uri uri}
                                  task-opts))))

(s/defn ^:always-validate write-bulk-tx-datoms-async
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms-async
                             :onyx/type :output
                             :onyx/medium :datomic
                             :onyx/doc "Transacts segments to storage asynchronously."}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-async-calls}]}
    :schema {:task-map DatomicWriteDatomsTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    task-opts :- {s/Any s/Any}]
   (write-bulk-tx-datoms-async task-name (merge {:datomic/uri uri}
                                                task-opts))))
