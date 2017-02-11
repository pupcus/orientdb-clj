(defproject orientdb-clj "0.1.1-SNAPSHOT"

  :description  "using an orientdb database in clojure"

  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [
                 [cheshire "5.7.0"]
                 [com.orientechnologies/orientdb-client "2.2.16"]
                 [com.orientechnologies/orientdb-core "2.2.16"]
                 [com.stuartsierra/component "0.3.2"]
                 [honeysql "0.8.2"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail javax.jms/jms com.sun.jdmk/jmxtools com.sun.jmx/jmxri]]
                 [org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 ])
