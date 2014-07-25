## onyx-datomic

Onyx plugin providing read and write facilities for batch processing a Datomic database.

#### Installation

In your project file:

```clojure
[com.mdrogalis/onyx-datomic "0.3.0"]
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
 :datomic/datoms-per-segment n
 :onyx/batch-size batch-size
 :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms"}
```

##### load-datoms

```clojure
{:onyx/name :load-datoms
 :onyx/ident :datomic/load-datoms
 :onyx/fn :onyx.plugin.datomic/load-datoms
 :onyx/type :transformer
 :onyx/consumption :concurrent
 :datomic/uri db-uri
 :datomic/t t
 :onyx/batch-size batch-size
 :onyx/doc "Reads and enqueues a range of the :eavt datom index"}
```

##### commit-tx

```clojure
{:onyx/name :output
 :onyx/ident :datomic/commit-tx
 :onyx/type :output
 :onyx/medium :datomic
 :onyx/consumption :concurrent
 :datomic/uri db-uri
 :onyx/batch-size batch-size
 :onyx/doc "Transacts :datoms to storage"}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/t`                  | `integer` | The t-value of the database to read from
|`:datomic/partition`          | `keyword` | The partition of the database to read out of
|`:datomic/datoms-per-segment` | `integer` | The number of datoms to compress into a single segment

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2014 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
