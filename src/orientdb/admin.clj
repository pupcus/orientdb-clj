(ns orientdb.admin
  (:import com.orientechnologies.orient.client.remote.OServerAdmin))

(defn exists? [admin db-name]
  (let [databases (.listDatabases admin)]
    (get databases db-name)))

(defn drop! [admin db-name]
  (if-let [url (exists? admin db-name)]
    (.dropDatabase admin db-name)
    (throw (Exception. (format "attempt to drop database that does NOT exist: '%s'" db-name)))))

(defn create!
  ([admin db-name] (create! admin db-name {}))
  ([admin db-name {:as opts :keys [type mode] :or {type "docmument" mode "plocal"}}]
   (if-let [url (exists? admin db-name)]
     (throw (Exception. (format "attempt to create database that alaredy exists: '%s' - %s" db-name url)))
     (.createDatabase admin db-name type mode))))

(defn get-administrator [db-subname user password]
  (doto (com.orientechnologies.orient.client.remote.OServerAdmin. db-subname)
    (.connect user password)))

(defn databases [url user password]
  (with-open [admin (get-administrator url user password)]
    (.listDatabases admin)))
