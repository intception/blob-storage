(defproject blob-storage "0.1.0"
  :description "Blob Storage Library"
  :url "https://github.com/intception/blob-storage"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [postgresql "9.3-1102.jdbc41"]
                 [sqlingvo "0.7.15"]]
  :aot [blob-storage.postgres])
