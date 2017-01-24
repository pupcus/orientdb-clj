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

(defrecord DataSource [user password dbtype hostname database]
  component/Lifecycle
  (start [component]
    (log/infof "Starting orientdb datasource type [%s] for database [%s]" dbtype database)
    (let [subname (build-subname component)
          factory (com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory.)
          pool (.get factory subname user password)]
      (with-meta (assoc component :pool pool :factory factory :subname subname :password "hidden") {:password password})))

  (stop [{:keys [factory dbtype database] :as component}]
    (log/infof "Shutting down orientdb datasource type [%s] for db [%s]" dbtype database)
    (.close factory)
    (-> (dissoc component :db :pool :factory :subname :passwd)
        (assoc component :password (:password (meta component))))))

(defn add-connection [connection db]
  (assoc db :connection connection))

(defn get-connection [{:keys [connection] :as db}]
  connection)

(defn new-connection [{:keys [pool]}]
  (let [con (.acquire pool)]
    (.set com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal/INSTANCE con)
    con))

(defmacro with-db-connection [binding & body]
  `(let [f# (^{:once true} fn* [~(first binding)] ~@body)
         db-spec# ~(second binding)]
     (if-let [exists# (get-connection db-spec#)]
       (f# db-spec#)
       (with-open [con# (new-connection db-spec#)]
         (log/debug "opening new connection ....")
         (let [~(first binding) (add-connection con# db-spec#)]
           (f# ~(first binding)))))))

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
  (with-db-connection [c db]
    (-> (.command (get-connection c) (com.orientechnologies.orient.core.sql.OCommandSQL. (first sql-v)))
        (.execute (to-array (rest sql-v))))))

(defn query [db sql-v]
  (let [rs (execute! db sql-v)]
    (mapv ODocument->map (.toArray rs))))

(defn get-document-instance [db table]
  (with-db-connection [c db]
    (.newInstance (get-connection db) (name table))))

(defn document! [db table m]
  (with-db-connection [c db]
    (let [document (doto (ODocument. (name table))
                     (.fromJSON (generate-string m)))]
      (.save (get-connection c) document)))  )
