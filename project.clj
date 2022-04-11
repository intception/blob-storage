(defproject com.intception/blob-storage "0.3.1"
  :description "Blob Storage Library"
  :url "https://github.com/intception/blob-storage"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.1.4.jre7"]
                 [com.microsoft.sqlserver/sqljdbc4 "4.2"]
                 [com.palletops/thread-expr "1.3.0"]
                 [sqlingvo "0.7.10"]])
