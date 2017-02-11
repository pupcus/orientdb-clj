(ns orientdb.utils
  (:refer-clojure :exclude [remove])
  (:require [honeysql.core :as sql]
            [honeysql.helpers :as helpers]
            [honeysql.format :as fmt]
            [cheshire.core :as json]
            [clojure.string :as str]))

;; -----
;; honeysql extensions for orientdb
;;

(defmethod fmt/format-clause :delete-vertex [[op v] sqlmap]
  (str "DELETE VERTEX " (fmt/to-sql v)))

(helpers/defhelper delete-vertex [m args]
  (assoc m :delete-vertex (first args)))

(fmt/register-clause! :delete-vertex 20)

(defmethod fmt/format-clause :remove [[op v] sqlmap]
  (str "REMOVE " (fmt/to-sql v)))

(helpers/defhelper remove [m args]
  (assoc m :remove (first args)))

(fmt/register-clause! :remove 75)

(defn remove-property-map [table property & c]
  (reduce criteria
          (-> (helpers/update table)
              (remove property))
          c))

(defmethod fmt/format-clause :content [[op v] sqlmap]
  (str "CONTENT " (json/encode v)))

(helpers/defhelper content [m args]
  (assoc m :content (first args)))

(fmt/register-clause! :content 90)

(defmethod fmt/fn-handler "contains" [op field predicate]
  (str (fmt/to-sql field) " CONTAINS " (fmt/format-predicate* predicate)))

;; ----
;; helper functions
;;

(defn dissoc-re
  "dissoc any key from the Map for which the string representation
  matches the given regular expression"
  [re m]
  (let [filter-fn (fn [m k]
                    (if (re-find re (name k))
                      (dissoc m k)
                      m))]
    (reduce filter-fn m (keys m))))

(def dissoc-meta (partial dissoc-re #"[@].*"))

(defn filter-rec [f rec]
  (apply
   merge
   (keep f rec)))

(defn filter-meta [rec]
  (filter-rec
   (fn [[k v]]
     (let [ks (name k)]
       (when (re-matches #"[@].*" ks)
         {k v})))
   rec))

(defn filter-collection-entries [rec]
  (filter-rec
   (fn [[k v]]
     (when (coll? v)
       {k v}))
   rec))

(defn remove-collection-entries [rec]
  (filter-rec
   (fn [[k v]]
     (when-not (coll? v)
       {k v}))
   rec))

;; -----
;; criteria
;;

(defn- criteria-dispatch [_ c]
  (class c))

(defmulti criteria #'criteria-dispatch)

(defmethod criteria clojure.lang.APersistentMap [m c]
  (reduce criteria m c))

(defmethod criteria clojure.lang.APersistentVector [m c]
  (if (= (count c) 2)
    (helpers/merge-where m (into [:=] c))
    (helpers/merge-where m c)))

(defmethod criteria :default [_ c]
  (throw (IllegalStateException. "Ill formed criteria:  [%s]" c)))


;; ----
;; honeysql map building
;;

(defn query-map
  "build a honeysql map to select * from table reducing criteria to a where clause"
  [table selection & c]
  (reduce criteria
          (-> (helpers/select selection)
              (helpers/from   table))
          c))

(defn select-map
  "build a honeysql map to select * from table reducing criteria to a where clause"
  [table & c]
  (apply query-map table :* c))

(defn exists-map
  "build a honeysql map to select * from table reducing criteria to a where clause"
  [table & c]
  (apply query-map table [:%count.* :n] c))

(defn set-with-content [m]
  (str/join
   ", "
   (for [[k v] m]
     (str (name k) " = " (json/encode v)))))

(defn build-update-sql-v [table m & c]
  (let [collection-entries (filter-collection-entries m)
        other-entries (remove-collection-entries m)

        [where-clause & where-args] (sql/format (reduce criteria {} c))

        [initial-update & other-args] (sql/format (cond-> (helpers/update table)
                                                    (seq other-entries) (helpers/sset other-entries)))

        set-with-content (when (seq collection-entries)
                           (str
                            (if (seq other-entries) "," " SET ")
                            (set-with-content collection-entries)))

        pieces (clojure.core/remove
                empty?
                [initial-update set-with-content where-clause])]

    (vec
     (reduce
      concat
      [[(str/join " " pieces)]
       other-args
       where-args]))))

(defn delete-map
  "build a honeysql map to delete from table reducing criteria to a where clause"
  [table & c]
  (reduce criteria (delete-vertex table) c))
