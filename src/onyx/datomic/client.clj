(ns onyx.datomic.client
  (:require [clojure.string :as str]
            [datomic.client.api :as d]
            [onyx.datomic.protocols :as dp]
            [taoensso.timbre :as log])
  (:import [java.net URI]))

(def tempid-counters (atom -1000000))

(defn- _cloud-client [{:keys [datomic-cloud/system
                              datomic-cloud/region
                              datomic-cloud/query-group
                              datomic-cloud/endpoint
                              datomic-cloud/proxy-port] :as datomic-config}]
  (let [region (or region (System/getenv "DATOMIC_CLOUD_REGION"))
        system (or system (System/getenv "DATOMIC_CLOUD_SYSTEM"))
        proxy-port (or proxy-port (System/getenv "DATOMIC_CLOUD_PROXY_PORT"))
        proxy-port (when (string? proxy-port) (Integer/parseInt proxy-port))]
    (d/client  {:server-type :cloud
                :region region
                :system system
                :query-group (or query-group (System/getenv "DATOMIC_CLOUD_QUERY_GROUP") system)
                :endpoint (or endpoint (format "http://entry.%s.%s.datomic.net:8182/" system region))
                :proxy-port (or proxy-port 8182)})))

(defn- _peer-server-client [{:keys [datomic-client/access-key
                                    datomic-client/secret
                                    datomic-client/endpoint] :as datomic-config}]
  (let [access-key (or access-key (System/getenv "DATOMIC_CLIENT_ACCESS_KEY"))
        secret (or secret (System/getenv "DATOMIC_CLIENT_SECRET"))
        endpoint (or endpoint (System/getenv "DATOMIC_CLIENT_ENDPOINT"))]
    (d/client  {:server-type :peer-server
                :access-key access-key
                :secret secret
                :endpoint endpoint})))

(def cloud-client (memoize _cloud-client))
(def peer-server-client (memoize _peer-server-client))

(defn- safe-connect-cloud [{:keys [datomic-cloud/db-name] :as datomic-config}]
  (when (nil? db-name)
    (throw (ex-info "either :datomic-cloud/db-name is required to connect." datomic-config)))
  (d/connect (cloud-client datomic-config) {:db-name db-name}))

(defn- safe-connect-client [{:keys [datomic-client/db-name] :as datomic-config}]
  (when (nil? db-name)
    (throw (ex-info ":datomic-client/db-name is required to connect." datomic-config)))
  (log/info "Connecting to database " db-name)
  (d/connect (peer-server-client datomic-config) {:db-name db-name}))

(def not-implemented-yet #(throw (ex-info "not implmented yet" {})))

(defrecord DatomicClient [lib-type]
  dp/DatomicHelpers
  (cas-key [_] :db/cas)
  (create-database [_ datomic-config]
    (if (= :cloud lib-type)
      (d/create-database (cloud-client datomic-config) {:db-name (:datomic-cloud/db-name datomic-config)})
      (throw (ex-info "Datomic client for On-Prem doesn't support create-database. Use peer API." datomic-config))))
  (delete-database [_ datomic-config]
    (if (= :cloud lib-type)
      (d/delete-database (cloud-client datomic-config) {:db-name (:datomic-cloud/db-name datomic-config)})
      (throw (ex-info "Datomic client for On-Prem doesn't support delete-database. Use peer API." datomic-config))))
  (instance-of-datomic-function? [this v] false)
  (next-t [_ db]
    (:next-t db))
  (safe-connect [_ datomic-config]
    (if (= :cloud lib-type)
      (safe-connect-cloud datomic-config)
      (safe-connect-client datomic-config)))
  (safe-as-of [_ datomic-config conn]
    (if-let [t (:datomic/t datomic-config)]
      (d/as-of (d/db conn) t)
      (throw (ex-info ":datomic/t missing from write-datoms datomic-config." datomic-config))))
  (transact [this conn data]
    (d/transact conn {:tx-data data}))
  (transact-async [this conn data]
    (future (d/transact conn {:tx-data data})))

  dp/DatomicFns
  (as-of [_] d/as-of)
  (datoms [_] (fn [db index & components]
                (let [arg-map (if (empty? components)
                                {:index index}
                                {:index index :components components})]
                  (d/datoms db arg-map))))
  (db [_] d/db)
  (entity [_] (fn [db eid] (d/pull db '[*] eid)))
  (ident [_] (fn [db eid] (-> (d/pull db '[:db/ident] eid)
                              :db/ident)))
  (index-range [_] (fn [db attrid start end]
                     (d/index-range db {:attrid attrid :start start :end end})))
  (q [_] d/q)
  (tempid [_] (fn [partition & n] (swap! tempid-counters dec)))
  (tx-range [_] (fn [conn start-tx & end-tx]
                  (if (empty? end-tx)
                    (d/tx-range conn {:start start-tx})
                    (d/tx-range conn {:start start-tx :end (first end-tx)})))))

(defn new-datomic-impl [lib-type]
  (->DatomicClient lib-type))
