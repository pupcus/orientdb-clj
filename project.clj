(defproject orientdb-clj "0.0.4-SNAPSHOT"

  :description  "using an orientdb database in clojure"

  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [
                 [cheshire "5.5.0"]
                 [com.orientechnologies/orientdb-client "2.2.14"]
                 [com.orientechnologies/orientdb-core "2.2.14"]
                 [com.stuartsierra/component "0.3.1"]
                 [honeysql "0.6.1"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail javax.jms/jms com.sun.jdmk/jmxtools com.sun.jmx/jmxri]]
                 [org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/tools.nrepl "0.2.10"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 ])
