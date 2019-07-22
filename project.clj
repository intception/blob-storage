(defproject com.intception/blob-storage "0.2.6"
  :description "Blob Storage Library"
  :url "https://github.com/intception/blob-storage"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.3.5"]
                 [postgresql/postgresql "8.4-702.jdbc4"]
                 [com.microsoft.sqlserver/sqljdbc4 "4.2"]
                 [sqlingvo "0.7.10"]])
