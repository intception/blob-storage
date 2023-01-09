(ns blob-storage.sqlserver-test
  (:use [clojure.test])
  (:require [blob-storage.sql-server :as p]
            [blob-storage.api :as b]
            [blob-storage.test-common :refer :all]
            [environ.core :refer [env]]))

(def db-spec (env :database-url-mssql))

(def service (p/make db-spec))

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


