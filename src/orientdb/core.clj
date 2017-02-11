(ns orientdb.core
  (:require [cheshire.core :refer :all]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [honeysql.core :as sql]
            [honeysql.helpers :as helpers]
            [orientdb.utils :as u])
  (:import (com.orientechnologies.orient.core.db ODatabaseRecordThreadLocal)
           (com.orientechnologies.orient.core.db.document ODatabaseDocumentTx)
           (com.orientechnologies.orient.core.record.impl ODocument)
           (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
           (com.orientechnologies.orient.core.tx OTransaction$TXTYPE)))

(defn- build-subname [{:keys [dbtype hostname database] :as options}]
  (cond
    (= "remote" dbtype)   (str dbtype ":" hostname "/" database)
    (= "plocal"  dbtype)  (str dbtype ":" "database/" database)
    (= "memory"  dbtype)  (str dbtype ":" "database/" database)
    :default  (throw (ex-info "invalid database type: must be 'remote', 'plocal', or 'memory" options))))

(defrecord DataSource [user password dbtype hostname database pool-size]
  component/Lifecycle
  (start [component]
    (log/infof "Starting orientdb datasource type [%s] for database [%s]" dbtype database)
    (let [subname (build-subname component)
          factory (com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory. (or pool-size 10))
          pool (.get factory subname user password)]
      (with-meta (assoc component :pool pool :factory factory :subname subname :password "hidden") {:password password})))

  (stop [{:keys [factory dbtype database] :as component}]
    (log/infof "Shutting down orientdb datasource type [%s] for db [%s]" dbtype database)
    (.close factory)
    (let [{:keys [password] :as meta-map} (meta component)]
      (-> (dissoc component :db :pool :factory :subname :passwd)
          (assoc component :password password)))))


(defn add-connection [connection db]
  (assoc db :connection connection))

(defn get-connection [{:keys [connection] :as db}]
  (when connection
    (.set com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal/INSTANCE connection)
    (.activateOnCurrentThread (cast com.orientechnologies.orient.core.db.document.ODatabaseDocument connection))))

(defn new-connection [{:keys [subname user] :as db}]
  (let [{:keys [password]} (meta db)
        connection (doto (com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx. subname)
                     (.open user password))]
    (.set com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal/INSTANCE connection)
    (.activateOnCurrentThread (cast com.orientechnologies.orient.core.db.document.ODatabaseDocument connection))))

(defmacro with-db-connection [binding & body]
  `(let [f# (^{:once true} fn* [~(first binding)] ~@body)
         db-spec# ~(second binding)]
     (if-let [exists# (get-connection db-spec#)]
       (f# db-spec#)
       (with-open [con# (new-connection db-spec#)]
         (let [new-db-spec# (add-connection con# db-spec#)]
           (f# new-db-spec#))))))

(defn- get-level [db]
  (get db :level 0))

(defn- inc-level [db]
  (let [level (get-level db)]
    (assoc db :level (inc level))))

;; ----
;; io! --> https://clojuredocs.org/clojure.core/io!
;;
;;
(defn db-transaction* [db func]
  (if (zero? (get-level db))
    (if-let [con (get-connection db)]
      (let [nested-db (inc-level db)]
        (io!
         (.begin con com.orientechnologies.orient.core.tx.OTransaction$TXTYPE/OPTIMISTIC)
         (try
           (let [result (func nested-db)]
             (.commit con)
             result)
           (catch Throwable t
             (prn t)
             (.rollback con)
             (throw t)))))
      (with-open [con (new-connection db)]
        (db-transaction* (add-connection con db) func)))
    (try
      (func (inc-level db))
      (catch Exception e
        (throw e)))))

(defmacro with-db-transaction [binding & body]
  `(db-transaction* ~(second binding) (^{:once true} fn* [~(first binding)] ~@body)))

(defn ODocument->map [document]
  (let [m (parse-string (.toJSON document) true)
        meta (u/filter-meta m)
        data (u/dissoc-meta m)]
    (with-meta data meta)))

(defn execute! [db sql-v]
  (log/debug sql-v)
  (with-db-connection [dbx db]
    (let [rs (as-> (first sql-v) %
               (com.orientechnologies.orient.core.sql.OCommandSQL. %)
               (.command (get-connection dbx) %)
               (.execute % (to-array (rest sql-v))))]
      (cond
        (instance? java.util.List rs)
        (mapv ODocument->map (.toArray rs))

        (instance? com.orientechnologies.orient.core.record.impl.ODocument rs)
        (ODocument->map rs)

        :otherwise
        rs))))

(defn query [db sql-v]
  (execute! db sql-v))

(defn get-document-instance [db table]
  (with-db-connection [c db]
    (.newInstance (get-connection db) (name table))))

(defn document! [db table m]
  (with-db-connection [c db]
    (let [document (doto (ODocument. (name table))
                     (.fromJSON (generate-string m)))]
      (.save (get-connection c) document)))  )
