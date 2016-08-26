(ns onyx.datomic.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.datomic/read-datoms
    {:summary "Reads datoms out of a Datomic database via `datomic.api/datoms`."
     :model {:datomic/uri
             {:type :string
              :doc "The URI of the datomic database to connect to."}

             :datomic/t
             {:type :integer
              :doc "The t-value of the database to read from."}

             :datomic/datoms-index
             {:type :keyword
              :doc "Datomic index to use in datomic.api/datoms call."}

             :datomic/datoms-components
             {:type :keyword
              :doc "Components of the datomic index to use (see datomic.api/datoms documentation). See the
[Clojure Cookbook](https://github.com/clojure-cookbook/clojure-cookbook/blob/master/06_databases/6-15_traversing-indices.asciidoc) for examples."}

             :datomic/datoms-per-segment
             {:type :integer
              :doc "The number of datoms to compress into a single segment."}

             :datomic/read-buffer
             {:type :integer
              :optional? true
              :default 1000
              :doc "The number of segments to buffer after partitioning."}}}

    :onyx.plugin.datomic/read-index-range
    {:summary "Reads datoms from an indexed attribute via `datomic.api/index-range`."
     :model {:datomic/uri
             {:type :string
              :doc "The URI of the datomic database to connect to."}

             :datomic/t
             {:type :integer
              :doc "The t-value of the database to read from."}

             :datomic/index-attribute
             {:type :keyword
              :doc "Datomic indexed attribute."}

             :datomic/index-range-start
             {:type :integer
              :doc "Inclusive start value for the index range."}

             :datomic/index-range-end
             {:type :integer
              :doc "Exclusive end value for the index range."}

             :datomic/datoms-per-segment
             {:type :integer
              :doc "The number of datoms to compress into a single segment."}

             :datomic/read-buffer
             {:type :integer
              :optional? true
              :default 1000
              :doc "The number of segments to buffer after partitioning."}}}

    :onyx.plugin.datomic/read-log
    {:summary "Reads the transaction log via repeated chunked calls of d/tx-range. Continues to read transactions until `:datomic/log-end-tx` is reached, or forever if `:datomic/log-end-tx` is nil."
     :model {:datomic/uri
             {:type :string
              :doc "The URI of the datomic database to connect to."}

             :datomic/log-start-tx
             {:type :integer
              :optional? true
              :doc "Starting tx (inclusive) for log read."}

             :datomic/log-end-tx
             {:type :integer
              :optional? true
              :doc "Ending tx (exclusive) for log read. Sentinel will emitted when this tx is passed."}

             :checkpoint/force-reset?
             {:type :boolean
              :doc "Whether or not checkpointing should be re-initialised from log-start-tx, or 0 in the case of nil."}

             :checkpoint/key
             {:type :uuid
              :optional? true
              :doc "Global (for a given onyx/tenancy-id) key under which to store the checkpoint information. By default the task-id for the job will be used, in which case checkpointing will only be resumed when a virtual peer crashes, and not when a new job is started."}

             :datomic/read-buffer
             {:type :integer
              :optional? true
              :default 1000
              :doc "The number of segments to buffer after partitioning."}}}

    :onyx.plugin.datomic/commit-tx
    {:summary "Writes new entity maps to datomic. Will automatically assign tempid's for the partition if a value for `:datomic/partition` is supplied and datomic transaction data is in map form. tx-data returned by `datomic.api/transact` is injected into the pipeline event map under `:datomic/written`."
     :model {:datomic/uri
             {:type :string
              :doc "The URI of the datomic database to connect to."}

             :datomic/partition
             {:type :keyword
              :optional? true
              :doc "When supplied, :db/id tempids are added using this partition."}}}

    :onyx.plugin.datomic/commit-bulk-tx
    {:summary "Exactly the same as commit-bulk-tx (asynchronous), but transacts each tx completely (blocking on the returned future) before proceeding to the next. You should generally prefer the async version."
     :model {:datomic/uri
             {:type :string
              :doc "The URI of the datomic database to connect to."}

             :datomic/partition
             {:type :keyword
              :optional? true
              :doc "When supplied, :db/id tempids are added using this partition."}}}

    :onyx.plugin.datomic/commit-bulk-tx-async
    {:summary "Writes transactions via the `:tx` segment key to a Datomic database. The value of `:tx` should be as if it were ready for `(d/transact uri tx)`. This lets you perform retractions and arbitrary db functions. tx-data returned by datomic.api/transact is injected into the pipeline event map under `:datomic/written`. Takes advantage of the Datomic transactor's ability to pipeline transactions by asynchronously transacting `:onyx/batch-size`transactions at once. Transaction futures are then derefed one by one after. Parallelism can thus be controlled by modifying the batch size appropriately. This is the recommended way to transact in bulk."
     :model {:datomic/uri
             {:type :string
              :doc "The URI of the datomic database to connect to."}

             :datomic/partition
             {:type :keyword
              :optional? true
              :doc "When supplied, :db/id tempids are added using this partition."}}}}

   :display-order
   {:onyx.plugin.datomic/read-datoms
    [:datomic/uri
     :datomic/t
     :datomic/datoms-index
     :datomic/datoms-components
     :datomic/datoms-per-segment
     :datomic/read-buffer]

    :onyx.plugin.datomic/read-index-range
    [:datomic/uri
     :datomic/t
     :datomic/index-attribute
     :datomic/index-range-start
     :datomic/index-range-end
     :datomic/datoms-per-segment
     :datomic/read-buffer]

    :onyx.plugin.datomic/read-log
    [:datomic/uri
     :datomic/log-start-tx
     :datomic/log-end-tx
     :checkpoint/force-reset?
     :checkpoint/key
     :datomic/read-buffer]

    :onyx.plugin.datomic/commit-tx
    [:datomic/uri
     :datomic/partition]

    :onyx.plugin.datomic/commit-bulk-tx
    [:datomic/uri
     :datomic/partition]

    :onyx.plugin.datomic/commit-bulk-tx-async
    [:datomic/uri
     :datomic/partition]}

   :lifecycle-entry
   {:onyx.plugin.datomic/read-datoms
    {:model
     [{:task.lifecycle/name :read-datoms
       :lifecycle/calls :onyx.plugin.datoms/read-datoms-calls}]}

    :onyx.plugin.datomic/read-index-range
    {:model
     [{:task.lifecycle/name :read-index-datoms
       :lifecycle/calls :onyx.plugin.datoms/read-index-range-calls}]}

    :onyx.plugin.datomic/read-log
    {:model
     [{:task.lifecycle/name :read-log
       :lifecycle/calls :onyx.plugin.datoms/read-log-calls}]}

    :onyx.plugin.datomic/commit-tx
    {:model
     [{:task.lifecycle/name :write-datoms
       :lifecycle/calls :onyx.plugin.datoms/write-tx-calls}]}

    :onyx.plugin.datomic/commit-bulk-tx
    {:model
     [{:task.lifecycle/name :write-bulk-datoms
       :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-calls}]}

    :onyx.plugin.datomic/commit-bulk-tx-async
    {:model
     [{:task.lifecycle/name :write-bulk-datoms-async
       :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-async-calls}]}}})
