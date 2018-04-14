(ns orientdb.utils
  (:refer-clojure :exclude [remove])
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [honeysql.core :as sql]
            [honeysql.format :as fmt]
            [honeysql.helpers :as helpers]))

(def _CLASS   (keyword "@class"))
(def _RID     (keyword "@rid"))
(def _TYPE    (keyword "@type"))
(def _VERSION (keyword "@version"))

(defn class [rec]
  (get (meta rec) _CLASS))

(defn rid [rec]
  (get (meta rec) _RID))

(defn type [rec]
  (get (meta rec) _TYPE))

(defn version [rec]
  (get (meta rec) _VERSION))

(defn ensure-vector [x]
  (if (vector? x)
    x
    [x]))

;; -----
;; honeysql extensions for orientdb
;;

(defmethod fmt/format-clause :delete-vertex [[op v] sqlmap]
  (str "DELETE VERTEX " (fmt/to-sql v)))

(helpers/defhelper delete-vertex [m args]
  (assoc m :delete-vertex (first args)))

(fmt/register-clause! :delete-vertex 20)

(defmethod fmt/format-clause :olet [[op v] sqlmap] {:pre [(even? (count (first v)))]}
  (str "LET "
       (fmt/comma-join
        (for [[_var clause] (partition 2 (first v))]
          (str "$" (name _var) " = "
               (fmt/comma-join (map fmt/to-sql [clause])))))))

(helpers/defhelper olet [m args]
  (assoc m :olet (helpers/collify args)))

(fmt/register-clause! :olet 90)

(defmethod fmt/format-clause :traverse [[op v] sqlmap]
  (str "TRAVERSE "
       (when (:modifiers sqlmap)
         (str (fmt/space-join (map (comp str/upper-case name)
                                   (:modifiers sqlmap)))
              " "))
       (fmt/comma-join (map fmt/to-sql v))))

(helpers/defhelper traverse [m args]
  (assoc m :traverse (helpers/collify args)))

(fmt/register-clause! :traverse 50)

(defmethod fmt/format-clause :remove [[op v] sqlmap]
  (str "REMOVE " (fmt/to-sql v)))

(helpers/defhelper remove [m args]
  (assoc m :remove (first args)))

(fmt/register-clause! :remove 75)

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

(defn remove-property-map [table property & c]
  (reduce criteria
          (-> (helpers/update table)
              (remove property))
          c))


(defn try-json
  "given a string, try to decode it otherwise return nil"
  [s]
  (try
    (when-not (empty? s)
      (json/decode s true))
    (catch Throwable t)))

(defn maybe-json
  "given a string, try to decode it otherwise return original string"
  [s]
  (or (try-json s) s))


;; ----
;; index all returned records by rid for easy lookup

(declare index-all-returned-records)

(defn scalar? [v]
  (or (number? v)
      (string? v)
      (boolean? v)
      (decimal? v)))

(defn build-indexed-record-dispatch-fn [index-tree [k v] path index]
  (cond
    (scalar? v)  :scalar
    (map? v)     :map
    (vector? v)  :vector
    :otherwise   (throw (ex-info "unexpected type in result map" {:value v}))))

(defmulti build-indexed-record #'build-indexed-record-dispatch-fn)

(defmethod build-indexed-record :scalar [[i t] [k v] path index]
  (let [m (get i index {})]
    [(assoc i index (assoc m k v)) t]))

(defmethod build-indexed-record :map [[i t] [k v] path index]
  (let [has-index? (get v _RID)
        m (get i index {})]
    (if has-index?
      (let [[new-index new-tree] (index-all-returned-records v)]
        [(assoc (merge i new-index) index (assoc m k has-index?)) (assoc-in t path new-tree)])
      [(assoc i index (assoc m k v)) t])))

(defmethod build-indexed-record :vector [[i t] [k v] path index]
  (let [[ni idv] (reduce
                  (fn [[i  v] e]
                    (let [m (get i index)]
                      (cond

                        (scalar? e)
                        (let [nv (conj v e)]
                          [(assoc i index (assoc m k nv)) nv])

                        (map? e)
                        (let [has-index? (get e _RID)]
                          (if has-index?
                            (let [[ni nt] (index-all-returned-records e)
                                  nv (conj v has-index?)]
                              [(assoc (merge i ni) index (assoc m k nv)) nv])
                            (let [nv (conj v e)]
                              [(assoc i index (assoc m k nv)) nv])))

                        :otherwise (do (println "ERROR: what to do with this in the vector? "v) [i v]))))

                  [i []]
                  v)]
    [ni (assoc-in t path idv)]))

(defn index-all-returned-records [results]
  (let [index (or (rid results) (get results _RID))]
    (if index
      (reduce
       (fn [it [k v :as kv]]
         (build-indexed-record it kv [k] index))
       [{} results]
       results)
      results)))

