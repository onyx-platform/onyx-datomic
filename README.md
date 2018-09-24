## onyx-datomic

Onyx plugin providing read and write facilities for batch and stream processing on a Datomic database.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-datomic "0.13.5.0"]
```
In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.datomic])
```

#### Datomic editions
Datomic Cloud edition was added in Jan. 2018 and the original Datomic 
was re-branded as Datomic On-Prem. 

Datomic On-Prem can be accessed via Peer API and Client API, while Datomic Cloud is 
accessible only via Client API. 

This plugin supports Datomic Peer API for Datomic On-Prem and Client API for Datomic Cloud. 
Client API is almost identical between On-Prem and Cloud other than connection mechanism, but 
some functions such as `create-database` is not supported for On-Prem. In addition to that, 
client API is not supported in Datomic On-Prem Free edition so it is not testable without a license and setting up of 
external transactor and peer-server. 

For those reasons, client API is tested only against Datomic Cloud.
Basic manual tests showed positive results on accessing to Datomic On-Prem Pro edition via Client API, but it is not 
continuously tested and may be broken. 

#### Define Datomic dependency

The behavior is dynamically determined by looking up a datomic lib in classpath. 
Add one of peer, client, or cloud library in the classpath. Including multiple libs is not supported.

##### Peer lib 
Add `com.datomic/datomic-free` or `com.datomic/datomic-pro`. 
Note that the registration to [My Datomic](https://my.datomic.com/login) is required to 
download Datomic Pro. The credential and repository needs to be set up in your project. 
See [My Account](https://my.datomic.com/account) page for the details. 

```clojure
[com.datomic/datomic-free "0.9.5544"]
```
##### Client lib for Datomic On-Prem
Add `com.datomic/client-pro` to your dependencies. 
See [Project Configuration](https://docs.datomic.com/on-prem/project-setup.html#project-configuration)
for the details.

###### Client lib for Datomic Cloud
Follow the instruction in [Datomic Cloud documentation](https://docs.datomic.com/cloud/getting-started/connecting.html#add-dependency) 
and add `com.datomic/client-cloud` to your project.

#### Attributes for Datomic Cloud Client API
Specify the following attributes to task map to connect Datomic Cloud. 

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic-cloud/region`       | `string`  | AWS region id where Datomic Cloud runs. e.g. `us-east-1`.
|`:datomic-cloud/system`       | `string`  | System name chosen on setting up Datomic Cloud.
|`:datomic-cloud/query-group`  | `string`  | Query group name if it is set up. Specify system name otherwise.
|`:datomic-cloud/endpoint`     | `string`  | Optional. The default is `http://entry.<system-name>.<region>.datomic.net:8182/`
|`:datomic-cloud/proxy-port`   | `string`  | Optional. Local port number for SSH tunnel to bastion. Default is 8182.
|`:datomic-cloud/db-name`      | `string`  | Database name.

`:datomic/uri` is ignored. 

#### Attributes for Datomic On-Prem Client API 
Specify the following attributes to task map to connect Datomic On-Prem via Client API. Note that it is not tested
due to the license and functional limitations described above.

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic-client/access-key`  | `string`  | Access key specified in Peer-server command line options.
|`:datomic-client/secret`      | `string`  | Secret specified in Peer-server command line options.
|`:datomic-client/endpoint`    | `string`  | `hostname:port-number` of Peer-server.
|`:datomic-client/db-name`     | `string`  | Database name.

`:datomic/uri` is ignored. 

#### Functions

##### read-datoms

Reads datoms out of a Datomic database via `datomic.api/datoms`.

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

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/t`                  | `integer` | The t-value of the database to read from
|`:datomic/datoms-index`       | `keyword` | datomic index to use in datomic.api/datoms call
|`:datomic/datoms-components`  | `keyword` | components of the datomic index to use (see datomic.api/datoms documentation)
|`:datomic/datoms-per-segment` | `integer` | The number of datoms to compress into a single segment
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`


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
 :datomic/index-range-start <<INDEX_START_VALUE>>
 :datomic/index-range-end <<INDEX_END_VALUE>>
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

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/t`                  | `integer` | The t-value of the database to read from
|`:datomic/index-attribute`    | `keyword` | datomic indexed attribute
|`:datomic/index-range-start`  | `any`     | inclusive start value for the index range
|`:datomic/index-range-end`    | `any`     | exclusive end value for the index range
|`:datomic/datoms-per-segment` | `integer` | The number of datoms to compress into a single segment
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`

##### read-log

Reads the transaction log via repeated chunked calls of d/tx-range. Continues
to read transactions until `:datomic/log-end-tx` is reached, or forever if
`:datomic/log-end-tx` is nil.

Catalog entry:

```clojure
{:onyx/name :read-log
 :onyx/plugin :onyx.plugin.datomic/read-log
 :onyx/type :input
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/log-start-tx <<OPTIONAL_TX_START_INDEX>>
 :datomic/log-end-tx <<OPTIONAL_TX_END_INDEX>>
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads a sequence of datoms from the d/log API"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-log
 :lifecycle/calls :onyx.plugin.datomic/read-log-calls}
```

Task will emit a sentinel `:done` when it reaches the tx log-end-tx
(exclusive).

Segments will be read in the form `{:t tx-id :data [[e a v t added] [e a v t added]]}`.

To recover read-log offsets in a new job, please use the onyx [resume points feature](http://www.onyxplatform.org/docs/user-guide/0.10.x/#resume-point).

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/log-start-tx`       | `integer` | optional starting tx (inclusive) for log read
|`:datomic/log-end-tx`         | `integer` | optional ending tx (inclusive) for log read. Sentinel will emitted when this tx is passed.
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`

##### commit-tx

Writes new entity maps to datomic. Will automatically assign tempid's for the partition
if a value for :datomic/partition is supplied and datomic transaction data is in map form.

Catalog entry:

```clojure
{:onyx/name :write-datoms
 :onyx/plugin :onyx.plugin.datomic/write-datoms
 :onyx/type :output
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/partition :my.database/optional-partition-name
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-datoms
 :lifecycle/calls :onyx.plugin.datomic/write-tx-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/partition`          | `keyword` | Optional keyword. When supplied, :db/id tempids are added using this partition.

##### commit-bulk-tx (asynchronous)

Writes transactions via the `:tx` segment key to a Datomic database. The value
of `:tx` should be as if it were ready for `(d/transact uri tx)`. This lets you
perform retractions and arbitrary db functions. Takes advantage of the Datomic transactor's ability to
pipeline transactions by asynchronously transacting `:onyx/batch-size`
transactions at once. Transaction futures are then derefed one by one after.
Parallelism can thus be controlled by modifying the batch size appropriately.
This is the recommended way to transact in bulk.

Catalog entry:

```clojure
{:onyx/name :write-bulk-datoms-async
 :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms-async
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
{:lifecycle/task :write-bulk-datoms-async
 :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-async-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/partition`          | `keyword` | Optional keyword. When supplied, :db/id tempids are added using this partition.

##### commit-bulk-tx (synchronous)

Exactly the same as commit-bulk-tx (asynchronous), but transacts each tx completely (blocking on the returned future) before proceeding to the next. You should generally prefer the async version.

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

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/partition`          | `keyword` | Optional keyword. When supplied, :db/id tempids are added using this partition.

##### Datomic Params Injection via Lifecycles

The datomic params lifecycles inject datomic dbs or conns into `:onyx/fn` params.

###### inject-conn

Injects a datomic conn into the event map. Will also inject as an :onyx/fn param if :onyx/param? is true.

```clojure
{:lifecycle/task :use-conn-task
 :lifecycle/calls :onyx.plugin.datomic/inject-conn-calls
 :datomic/uri db-uri
 :onyx/param? true
 :lifecycle/doc "Initialises datomic conn as a :onyx.core/param"}
```

###### inject-db

Injects a datomic db into the event map. Will also inject as an :onyx/fn param if :onyx/param? is true.

`:datomic/basis-t` is optional, and if supplied it calls datomic.api/as-of on the db using `:datomic/basis-t`.

```clojure
{:lifecycle/task :use-db-task
 :lifecycle/calls :onyx.plugin.datomic/inject-db-calls
 :datomic/uri db-uri
 :datomic/basis-t optional-basis-t
 :onyx/param? true
 :lifecycle/doc "Initialises datomic db as a :onyx.core/param"}
```
#### Development
##### Run tests against Datomic Cloud
1. [Set up Datomic Cloud](https://docs.datomic.com/cloud/setting-up.html). 
2. [Allow inbound bastion traffic](https://docs.datomic.com/cloud/getting-started/configuring-access.html#authorize-bastion).
3. [Start a socks proxy](https://docs.datomic.com/cloud/getting-started/connecting.html#socks-proxy).
4. Update `com.datomic/client-cloud` version in `project.clj` if necessary.
5. Run tests compatible with Datomic cloud. 

```
lein with-profile cloud test :cloud
```


#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
