(ns orientdb.crud
  (:refer-clojure :exclude [find class type])
  (:require [clojure.string :as str]
            [honeysql.core :as sql]
            [honeysql.helpers :as h]
            [orientdb.core :as core]
            [orientdb.utils :as util]))

(defn select
  "select from table based on a sequence of criteria"
  [db table & criteria]
  (let [sql-v (sql/format (apply util/select-map table criteria))]
    (core/query db sql-v)))

(defn find [db table & criteria]
  (first (apply select db table criteria)))

(defn find-by-id [db table id]
  (find db table {util/_RID id}))

(defn exists?
  "check existence in table based on a sequence of criteria"
  [db table & criteria]
  (let [sql-v (sql/format (apply util/exists-map table criteria))
        {:keys [n]} (or (first (core/query db sql-v)) {:n 0})]
    (if (> n 0) n)))

(defn- generated-id [result]
  (util/rid result))

(defn insert! [db table m]
  (let [sql-m (-> (h/insert-into table)
                  (util/content m))
        sql-v (sql/format sql-m)]
    (core/execute! db sql-v)))

(def create! insert!)

(defn- update* [db table m & criteria]
  (if (empty? m)
    (throw (IllegalArgumentException. "no values to set on update"))
    (let [sql-v (apply util/build-update-sql-v table m criteria)]
      (core/execute! db sql-v))))

(defn update!
  "if m has an :@rid meta key, update using critera including [:= :@id id]
     use m as the set values for the rec in a transaction

   else if we have only criteria and no id in m
      use m as the set values for ANY recs matching criteria

   else use m as set values to update entire db (careful!)"
  [db table m & criteria]
  (if-let [id (util/rid m)]
    (let [criteria (cons {util/_RID id} criteria)]
      (apply update* db table m criteria)
      (find-by-id db table id))
    (apply update* db table m criteria)))

(defn upsert! [db table m]
  (if (util/rid m)
    (update! db table m)
    (insert! db table m)))

(defn delete!
  "delete according to a set of criteria"
  [db table & criteria]
  (if criteria
    (let [sql-v (sql/format (apply util/delete-map table criteria))]
      (core/execute! db sql-v))
    (throw (IllegalArgumentException. "no criteria found for delete"))))
