(ns onyx.datomic.peer
  (:require [clojure.string :as str]
            [onyx.datomic.protocols :as dp]
            [datomic.api :as d]))

(defrecord DatomicPeer [lib-type]
  dp/DatomicHelpers
  (cas-key [_] :db.fn/cas)
  (create-database [_ {:keys [datomic/uri] :as datomic-config}]
    (d/create-database uri))
  (delete-database [_ {:keys [datomic/uri] :as datomic-config}]
    (d/delete-database uri))
  (instance-of-datomic-function? [this v]
    (instance? datomic.function.Function v))
  (next-t [_ db]
    (d/next-t db))
  (safe-connect [_ datomic-config]
    (if-let [uri (:datomic/uri datomic-config)]
      (d/connect uri)
      (throw (ex-info ":datomic/uri missing from write-datoms datomic-config." datomic-config))))
  (safe-as-of [_ datomic-config conn]
    (if-let [t (:datomic/t datomic-config)]
      (d/as-of (d/db conn) t)
      (throw (ex-info ":datomic/t missing from write-datoms datomic-config." datomic-config))))
  (transact [_ conn data]
    @(d/transact conn data))
  (transact-async [_ conn data]
    (d/transact-async conn data))

  dp/DatomicFns
  (as-of [_] d/as-of)
  (datoms [_] d/datoms)
  (db [_] d/db)
  (entity [_] d/entity)
  (ident [_] d/ident)
  (index-range [_] d/index-range)
  (q [_] d/q)
  (tempid [_] d/tempid)
  (tx-range [_] (fn [conn start-tx]
                  (let [log (d/log conn)]
                    (d/tx-range log start-tx nil)))))

(defn new-datomic-impl [lib-type]
  (->DatomicPeer lib-type))
