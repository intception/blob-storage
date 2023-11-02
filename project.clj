(defproject com.intception/blob-storage "0.4.5"
  :description "Blob Storage Library"
  :url "https://github.com/intception/blob-storage"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "1.6.681"]
                 [org.clojure/core.cache "1.0.225"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.5.1"]
                 [com.microsoft.sqlserver/sqljdbc4 "4.2"]
                 [com.palletops/thread-expr "1.3.0"]
                 [sqlingvo "0.7.10"]]
  :profiles {:dev [:project/dev :profiles/dev]
             :profiles/dev {} ; profiles.clj (not in git)
             :project/dev {:dependencies [[environ "1.2.0"]]
                           :plugins [[lein-environ "1.2.0"]]}})

