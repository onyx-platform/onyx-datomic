(ns onyx.datomic.api
  (:require [clojure.string :as str]
            [onyx.datomic.protocols :as dp])
  (:import [java.io FileNotFoundException]
           [java.net URI]))

(try (require '[datomic.client.api :as d])
     (catch FileNotFoundException _ (require '[datomic.api :as d])))

(defn- _datomic-lib-type []
  (if (find-ns 'datomic.client.api)
    (try (require '[datomic.client.impl.cloud])
         :cloud
         (catch FileNotFoundException _ :client))
    :peer))

(def datomic-lib-type (memoize _datomic-lib-type))

(case (datomic-lib-type)
  :peer   (require '[onyx.datomic.peer :refer [new-datomic-impl]])
  :client (require '[onyx.datomic.client :refer [new-datomic-impl]])
  :cloud  (require '[onyx.datomic.client :refer [new-datomic-impl]]))

(defn client? []
  (or (= :cloud (datomic-lib-type))
      (= :client (datomic-lib-type))))

(defn peer? []
  (= :peer (datomic-lib-type)))

(defn cloud? []
  (= :cloud (datomic-lib-type)))

(defn- _datomic-lib []
  (new-datomic-impl (datomic-lib-type)))

(def datomic-lib (memoize _datomic-lib))

(defn db-name-in-uri [uri]
  (-> uri
      URI.
      .getSchemeSpecificPart
      (str/split #"/")
      last))
(def cas-key (partial dp/cas-key (datomic-lib)))
(def create-database (partial dp/create-database (datomic-lib)))
(def delete-database (partial dp/delete-database (datomic-lib)))
(def instance-of-datomic-function? (partial dp/instance-of-datomic-function? (datomic-lib)))
(def next-t (partial dp/next-t (datomic-lib)))
(def safe-connect (partial dp/safe-connect (datomic-lib)))
(def safe-as-of (partial dp/safe-as-of (datomic-lib)))
(def transact (partial dp/transact (datomic-lib)))
(def transact-async (partial dp/transact-async (datomic-lib)))

(def as-of (dp/as-of (datomic-lib)))
(def connect safe-connect)
(def datoms (dp/datoms (datomic-lib)))
(def db (dp/db (datomic-lib)))
(def entity (dp/entity (datomic-lib)))
(def ident (dp/ident (datomic-lib)))
(def index-range (dp/index-range (datomic-lib)))
(def q (dp/q (datomic-lib)))
(def tempid (dp/tempid (datomic-lib)))
(def tx-range (dp/tx-range (datomic-lib)))
