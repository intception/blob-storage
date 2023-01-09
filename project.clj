(defproject com.intception/blob-storage "0.4.1"
  :description "Blob Storage Library"
  :url "https://github.com/intception/blob-storage"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.1.4.jre7"]
                 [com.microsoft.sqlserver/sqljdbc4 "4.2"]
                 [com.palletops/thread-expr "1.3.0"]
                 [sqlingvo "0.7.10"]
                 ;; S3
                 [org.clojure/core.async "1.5.648"] ;; later versions require clojure 1.10
                 ;; note: requires new versions of org.eclipse.jetty/jetty-client and org.eclipse.jetty/jetty-util
                 ;; which are incompatible with prisma
                 [com.cognitect.aws/api "0.8.635"]
                 [com.cognitect.aws/endpoints "1.1.12.373"]
                 [com.cognitect.aws/s3 "825.2.1250.0"]]
  :profiles {:dev [:project/dev :profiles/dev]
             :profiles/dev {} ; profiles.clj (not in git)
             :project/dev {:dependencies [[environ "1.2.0"]]
                           :plugins [[lein-environ "1.2.0"]]}})
