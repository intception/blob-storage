(ns blob-storage.postgres-test
  (:require [blob-storage.api :as b]
            [blob-storage.backends.postgres :as backend]
            [blob-storage.core :as bs]
            [blob-storage.test-common :refer :all]
            [environ.core :refer [env]])
  (:use [clojure.test])
  (:import (java.io File)))

(def db-spec (env :database-url-pg))

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

(deftest store-large-object
  (with-redefs [blob-storage.postgres.schema/large-object-threshold (delay 100)]
    (testing "smaller than threshold"
      (let [blob (byte-array 99)
            blob-id (b/store! service blob)
            stored-blob (b/blob service blob-id)]
        (is (some? (get @(:state stored-blob) :bytes)) "Blob should have bytes")
        (is (nil? (get @(:state stored-blob) :file)) "Blob should not be a file")
        (is (= 99 (alength (b/get-bytes stored-blob))) "Incorrect blob length"))

      (let [file (File/createTempFile "test-blob-storage" ".bin")]
        (with-open [w (clojure.java.io/output-stream file)]
          (.write w (byte-array 99)))
        (try
          (let [blob-id (b/store! service file)
                stored-blob (b/blob service blob-id)]
            (is (some? (get @(:state stored-blob) :bytes)) "Blob should have bytes")
            (is (nil? (get @(:state stored-blob) :file)) "Blob should not be a file")
            (is (= 99 (alength (b/get-bytes stored-blob))) "Incorrect blob length")
            (is (= 99 (alength (stream->bytes (b/open-input-stream stored-blob)))) "Incorrect stream blob size"))
          (finally
            (.delete file)))))

    (testing "bigger than threshold"
      (let [blob (byte-array 512000)
            blob-id (b/store! service blob)
            stored-blob (b/blob service blob-id)]
        (is (nil? (get @(:state stored-blob) :bytes)) "Blob should not have bytes")
        (is (some? (get @(:state stored-blob) :file)) "Blob should be a file")
        (is (= 512000 (alength (b/get-bytes stored-blob))) "Incorrect blob length")
        (is (= 512000 (alength (stream->bytes (b/open-input-stream stored-blob)))) "Incorrect stream blob size"))

      (let [file (File/createTempFile "test-blob-storage" ".bin")]
        (with-open [w (clojure.java.io/output-stream file)]
          (.write w (byte-array 512000)))
        (try
          (let [blob-id (b/store! service file)
                stored-blob (b/blob service blob-id)]
            (is (nil? (get @(:state stored-blob) :bytes)) "Blob should not have bytes")
            (is (some? (get @(:state stored-blob) :file)) "Blob should be a file")
            (is (= 512000 (alength (b/get-bytes stored-blob))) "Incorrect blob length")
            (is (= 512000 (alength (stream->bytes (b/open-input-stream stored-blob)))) "Incorrect stream blob size"))
          (finally
            (.delete file)))))))

(deftest test-blob-cache
  (with-redefs [blob-storage.postgres.schema/large-object-threshold (delay 1000)]
    (test-local-cache (backend/make db-spec))))

