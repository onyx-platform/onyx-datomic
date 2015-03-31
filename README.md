## onyx-datomic

Onyx plugin providing read and write facilities for batch processing a Datomic database.

#### Installation

In your project file:

```clojure
[com.mdrogalis/onyx-datomic "0.6.0-SNAPSHOT"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.datomic])
```

#### Catalog entries

##### partition-datoms
```clojure
{:onyx/name :partition-datoms
 :onyx/ident :datomic/partition-datoms
 :onyx/type :input
 :onyx/medium :datomic
 :onyx/consumption :sequential
 :onyx/bootstrap? true
 :datomic/uri db-uri
 :datomic/t t
 :datomic/partition :com.my.example/partition
 :onyx/batch-size batch-size
 :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms"}
```

##### read-datoms

```clojure
{:onyx/name :read-datoms
 :onyx/ident :datomic/read-datoms
 :onyx/fn :onyx.plugin.datomic/read-datoms
 :onyx/type :function
 :onyx/consumption :concurrent
 :datomic/uri db-uri
 :datomic/partition my.datomic.partition
 :datomic/t t
 :onyx/batch-size batch-size
 :onyx/doc "Reads and enqueues a range of the :eavt datom index"}
```

##### commit-tx

The first variant expects to be fed in a stream of new entity maps and will automatically assign tempid's for the partition given.

```clojure
{:onyx/name :out
 :onyx/ident :datomic/commit-tx
 :onyx/type :output
 :onyx/medium :datomic
 :onyx/consumption :concurrent
 :datomic/uri db-uri
 :datomic/partition my.datomic.partition
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

The `:onyx/medium :datomic-tx` variant expects a tx, almost as if it was ready for `(d/transact uri tx)`. This lets you perform retractions and arbitrary db functions. 

```clojure
{:onyx/name :out
 :onyx/ident :datomic/commit-tx
 :onyx/type :output
 :onyx/medium :datomic-tx
 :onyx/consumption :concurrent
 :datomic/uri db-uri
 :datomic/partition my.datomic.partition
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```


Segments to be supplied to the :datomic/commit-tx out task in a form such as the following:

```clojure
(require '[datomic.api :as d])

{:tx [[:db/add (d/tempid :db.part/user) :db/doc "Hello world"]]}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/t`                  | `integer` | The t-value of the database to read from
|`:datomic/partition`          | `keyword` | The partition of the database to read out of
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
