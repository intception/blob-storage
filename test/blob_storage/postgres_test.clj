(ns blob-storage.postgres-test
  (:use [clojure.test])
  (:require [blob-storage.postgres :as p]
            [blob-storage.api :as b]
            [clojure.java.jdbc :as j]
            [sqlingvo.core :as sql]
            [sqlingvo.db :as sqdb])
  (:import (java.io ByteArrayInputStream FileInputStream)))


(def db-spec (or (System/getenv "DATABASE_URL")
                 "postgresql://postgres:postgres@localhost:5432/blobs_test"))

(def service (p/make db-spec))

(defn init-schema-fixture [f]
  (try
    (b/drop-schema! service)
    (catch Exception e))
  (b/init-schema! service)
  (f))

(use-fixtures :each init-schema-fixture)

(deftest store-blob
  (let [blob (byte-array [(byte 0) (byte 1) (byte 2)])
        blob-id (b/store! service blob)
        stored-blob (b/blob service blob-id)]

    (testing "store blob"
      (is (= (:id stored-blob) blob-id) "Different blob id from the stored")
      (is (not (nil? (:blob stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (:size stored-blob)) "Incorrect blob size")
      (is (= (aget (b/get-bytes (:blob stored-blob)) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (:created_at stored-blob))) "This blob doesn't have a created date")
      (is (nil? (:updated_at stored-blob)) "Newly created blobs doesn't have updated date"))))

(deftest update-blob
  (let [blob-id (b/store! service (byte-array [(byte 0) (byte 1) (byte 2)]))
        new-blob (byte-array [(byte 3)])
        _ (b/update! service blob-id new-blob)
        updated-blob (b/blob service blob-id)]

    (testing "update blob"
      (is (= (:id updated-blob) blob-id) "Different blob id from the updated")
      (is (not (nil? (:blob updated-blob))) "Blob updated incorrectly")
      (is (= (alength new-blob) (:size updated-blob)) "Incorrect blob size")
      (is (= (aget (b/get-bytes (:blob updated-blob)) 0) 3) "This blob doesn't seem like the one I stored")
      (is (not (nil? (:created_at updated-blob))) "This blob doesn't have a created date")
      (is (not (nil? (:updated_at updated-blob))) "This blob doesn't have a updated date"))))


(deftest delete-blob
  (let [blob-id (b/store! service (byte-array [(byte 0) (byte 1) (byte 2)]))
        _ (b/del! service blob-id)
        deleted-blob (b/blob service blob-id)]
    (testing "update blob"
      (is (nil? deleted-blob) "Found blob after deletion"))))

(deftest store-large-object
  (with-redefs [blob-storage.postgres.schema/large-object-threshold (delay 100)]
    (testing "smaller than threshold"
      (let [blob (byte-array 99)
            blob-id (b/store! service blob)
            stored-blob (b/blob service blob-id)]
        (is (b/bytea? (:blob stored-blob)) "Blob column should be filled with data")
        (is (= 99 (alength (b/get-bytes (:blob stored-blob)))) "Incorrect blob length")
        (is (instance? ByteArrayInputStream (b/get-stream (:blob stored-blob))) "Incorrect coercion to InputStream")))

    (testing "bigger than threshold"
      (let [blob (byte-array 101)
            blob-id (b/store! service blob)
            stored-blob (b/blob service blob-id)]
        (is (not (b/bytea? (:blob stored-blob))) "Blob should be afile")
        (is (= 101 (alength (b/get-bytes (:blob stored-blob)))) "Incorrect blob length" )
        (is (instance? FileInputStream (b/get-stream (:blob stored-blob))) "Incorrect coercion to InputStream")))))
