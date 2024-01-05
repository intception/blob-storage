(ns blob-storage.sqlserver-test
  (:require [blob-storage.api :as b]
            [blob-storage.backends.sql-server :as backend]
            [blob-storage.core :as bs]
            [blob-storage.test-common :refer :all]
            [environ.core :refer [env]])
  (:use [clojure.test]))

(def db-spec (env :database-url-mssql))
(def service (bs/make (backend/make db-spec) {}))

(defn init-schema-fixture [f]
  (try
    (b/drop-schema! service)
    (catch Exception e))
  (b/init-schema! service)
  (f))

(use-fixtures :each init-schema-fixture)

(deftest store-blob
  (test-store-blob service))

(deftest store-blob-with-id
  (test-store-blob-with-id service))

(deftest store-blob-with-tag
  (test-store-blob-with-tag service))

(deftest store-blob-with-id-and-tag
  (test-store-blob-with-id-and-tag service))

(deftest update-blob
  (test-update-blob service))

(deftest delete-blob
  (test-delete-blob service))

(deftest test-blob-cache
  (test-local-cache (backend/make db-spec)))

