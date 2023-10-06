(ns blob-storage.postgres-test
  (:require [blob-storage.api :as b]
            [blob-storage.coerce :as bc]
            [blob-storage.postgres :as p]
            [blob-storage.test-common :refer :all]
            [environ.core :refer [env]])
  (:use [clojure.test]))

(def db-spec (env :database-url-pg))

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

(deftest store-large-object
  (with-redefs [blob-storage.postgres.schema/large-object-threshold (delay 100)]
    (testing "smaller than threshold"
      (let [blob (byte-array 99)
            blob-id (b/store! service blob)
            stored-blob (b/blob service blob-id)
            blob-meta (b/blob-metadata service blob-id)]
        (is (instance? java.io.InputStream (:blob stored-blob)))
        (is (instance? java.io.BufferedInputStream (:blob stored-blob)))
        (is (nil? (:file stored-blob)))
        (is (= 99 (alength (bc/blob->bytes (:blob stored-blob)))) "Incorrect blob length")
        (is (= blob-meta (dissoc stored-blob :blob)))))

    (testing "bigger than threshold"
      (let [blob (byte-array 101)
            blob-id (b/store! service blob)
            stored-blob (b/blob service blob-id)
            blob-meta (b/blob-metadata service blob-id)]
        (is (not (bytes? (:blob stored-blob))) "Blob should be afile")
        (is (instance? java.io.InputStream (:blob stored-blob)))
        (is (instance? java.io.BufferedInputStream (:blob stored-blob)))
        (is (instance? java.io.File (:file stored-blob)))
        (is (= 101 (alength (bc/blob->bytes (:blob stored-blob)))) "Incorrect blob length")
        (is (= blob-meta (dissoc stored-blob :blob)))))))
