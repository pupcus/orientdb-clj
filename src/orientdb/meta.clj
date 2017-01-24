(ns orientdb.meta
  (:require [clojure.string :as str]
            [honeysql.core :as sql]
            [honeysql.helpers :as h]
            [orientdb.core :as core]
            [orientdb.utils :as util]))

(defn create-class!
  ([db table] (create-class! db table {}))
  ([db table {:keys [parent] :as opts}]
   (let [sql-v [(cond-> (str "CREATE CLASS " (name table))
                  parent (str " EXTENDS " (name parent)))]]
     (core/execute! db sql-v))))

(defn drop-class! [db table]
  (core/execute! db [(str "DROP CLASS " (name table))])
  (core/execute! db [(str "DROP CLUSTER " (name table))]))

(defn create-property!
  ([db table property] (create-property! db table property {}))
  ([db table property {:keys [type unsafe] :as opts :or {type "STRING" unsafe true}}]
   (let [sql-v [(cond-> (str "CREATE PROPERTY " (name table) "." (name property) " " (str/upper-case type))
                  unsafe (str " UNSAFE"))]]
     (core/execute! db sql-v))))

(defn drop-property! [db table property & criteria]
  (let [property-name (name property)
        target-class (-> (.getMetadata (core/get-connection db))
                         (.getSchema)
                         (.getClass (name table)))
        sql-v   (sql/format (apply util/remove-property-map table property criteria))]
    (when (.getProperty target-class property-name)
      (.dropProperty target-class property-name))
    (core/execute! db sql-v)))

(defn create-index!
  ([db table property] (create-index! db table property {}))
  ([db table property {:keys [unique] :as opts :or {unique false}}]
   (let [sql-v [(str "CREATE INDEX " (name table) "." (name property) (if unique " UNIQUE" " NOTUNIQUE"))]]
     (core/execute! db sql-v))))

(defn drop-index!
  ([db table property]
   (let [sql-v [(str "DROP INDEX " (name table) "." (name property))]]
     (core/execute! db sql-v))))
