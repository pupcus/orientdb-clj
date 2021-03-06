(defproject orientdb-clj "0.1.3-SNAPSHOT"

  :description "using an orientdb database in clojure"

  :url "https://github.com/pupcus/orientdb-clj"

  :scm {:url "git@github.com:pupcus/orientdb-clj.git"}

  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [
                 [cheshire "5.7.0"]
                 [com.orientechnologies/orientdb-client "2.2.19"]
                 [com.orientechnologies/orientdb-core "2.2.19"]
                 [com.stuartsierra/component "0.3.2"]
                 [honeysql "0.8.2"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail javax.jms/jms com.sun.jdmk/jmxtools com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 ]

  :profiles {:dev {:resource-paths ["dev-resources"]
                   :dependencies [
                                  [org.clojure/clojure "1.8.0"]
                                  [org.slf4j/slf4j-log4j12 "1.7.5"]
                                  ]}
             :test {:dependencies [[kosmos/kosmos-orientdb-server "0.0.4-SNAPSHOT"]]}}

  :deploy-repositories [["snapshots"
                         {:url "https://clojars.org/repo"
                          :creds :gpg}]
                        ["releases"
                         {:url "https://clojars.org/repo"
                          :creds :gpg}]]

  :global-vars {*warn-on-reflection* true
                *assert* false})
