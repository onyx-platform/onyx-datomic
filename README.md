## onyx-datomic

Onyx plugin providing read and write facilities for batch processing a Datomic database.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-datomic "0.7.0.1"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.datomic])
```

#### Functions

##### read-datoms

Reads datoms out a Datomic database via `datomic.api/datoms`.

Catalog entry:

```clojure
{:onyx/name :read-datoms
 :onyx/plugin :onyx.plugin.datomic/read-datoms
 :onyx/type :input
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/t t
 :datomic/datoms-index :eavt
 :datomic/datoms-components []
 :datomic/datoms-per-segment 20
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads a sequence of datoms from the d/datoms API"}
```

`:datomic/datoms-components` may be used to filter by a datomic index. See the 
[Clojure Cookbook](https://github.com/clojure-cookbook/clojure-cookbook/blob/master/06_databases/6-15_traversing-indices.asciidoc) for examples.

Lifecycle entry:

```clojure
{:lifecycle/task :read-datoms
 :lifecycle/calls :onyx.plugin.datomic/read-datoms-calls}
```

##### read-index-range

Reads datoms from an indexed attribute via `datomic.api/index-range`.

Catalog entry:

```clojure
{:onyx/name :read-index-datoms
 :onyx/plugin :onyx.plugin.datomic/read-index-range
 :onyx/type :input
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/t t
 :datomic/index-attribute :your-indexed-attribute
 :datomic/index-range-start "<<INDEX_START_VALUE>>"
 :datomic/index-range-end "<<INDEX_END_VALUE>>"
 :datomic/datoms-per-segment 20
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads a range of datoms from the d/index-range API"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-index-datoms
 :lifecycle/calls :onyx.plugin.datomic/read-index-range-calls}
```

##### commit-tx

Writes new entity maps and will automatically assign tempid's for the partition.

Catalog entry:

```clojure
{:onyx/name :write-datoms
 :onyx/plugin :onyx.plugin.datomic/write-datoms
 :onyx/type :output
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/partition :my.database/partition
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-datoms
 :lifecycle/calls :onyx.plugin.datomic/write-tx-calls}
```

##### commit-bulk-tx

Writes transactions via the `:tx` segment key to a Datomic database. The value of `:tx` should be as if it were ready for `(d/transact uri tx)`. This lets you perform retractions and arbitrary db functions. 

Catalog entry:

```clojure
{:onyx/name :write-bulk-datoms
 :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms
 :onyx/type :output
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/partition :my.database/partition
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

An example value of `:tx` would look like the following:

```clojure
(require '[datomic.api :as d])

{:tx [[:db/add (d/tempid :db.part/user) :db/doc "Hello world"]]}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-bulk-datoms
 :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-calls}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/t`                  | `integer` | The t-value of the database to read from
|`:datomic/partition`          | `keyword` | The partition of the database to read out of
|`:datomic/datoms-per-segment` | `integer` | The number of datoms to compress into a single segment
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
