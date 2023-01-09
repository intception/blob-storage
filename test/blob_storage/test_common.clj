(ns blob-storage.test-common
  (:require [blob-storage.api :as b]
            [blob-storage.coerce :as bc]
            [clojure.test :refer :all]))

(defn test-store-blob
  [service]
  (let [blob (byte-array [(byte 0) (byte 1) (byte 2)])
        blob-id (b/store! service blob)
        stored-blob (b/blob service blob-id)]
    (testing "store blob"
      (is (= (:id stored-blob) blob-id) "Different blob id from the stored")
      (is (not (nil? (:blob stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (:size stored-blob)) "Incorrect blob size")
      (is (some? (:blob stored-blob)) "Blob is nil")
      (is (= (aget (bc/blob->bytes (:blob stored-blob)) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (:created_at stored-blob))) "This blob doesn't have a created date"))))

(defn test-store-blob-with-id
  [service]
  (let [blob-id "pan casero"
        blob (byte-array [(byte 0) (byte 1) (byte 2)])
        returned-blob-id (b/store! service blob {:id blob-id})
        stored-blob (b/blob service blob-id)]
    (testing "store blob"
      (is (= (:id stored-blob) returned-blob-id blob-id) "Different blob id from the stored")
      (is (not (nil? (:blob stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (:size stored-blob)) "Incorrect blob size")
      (is (= (aget (bc/blob->bytes (:blob stored-blob)) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (:created_at stored-blob))) "This blob doesn't have a created date"))))

(defn test-store-blob-with-tag
  [service]
  (let [tag "test-tag"
        blob (byte-array [(byte 0) (byte 1) (byte 2)])
        blob-id (b/store! service blob {:tag tag})
        stored-blob (b/blob service blob-id)]
    (testing "store blob"
      (is (= (:id stored-blob) blob-id) "Different blob id from the stored")
      (is (= (:tag stored-blob) tag) "Different tag from the stored")
      (is (not (nil? (:blob stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (:size stored-blob)) "Incorrect blob size")
      (is (= (aget (bc/blob->bytes (:blob stored-blob)) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (:created_at stored-blob))) "This blob doesn't have a created date"))))

(defn test-store-blob-with-id-and-tag
  [service]
  (let [blob-id "pan casero"
        tag "test-tag"
        blob (byte-array [(byte 0) (byte 1) (byte 2)])
        returned-blob-id (b/store! service blob {:id blob-id :tag tag})
        stored-blob (b/blob service blob-id)]
    (testing "store blob"
      (is (= (:id stored-blob) returned-blob-id blob-id) "Different blob id from the stored")
      (is (= (:tag stored-blob) tag) "Different tag from the stored")
      (is (not (nil? (:blob stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (:size stored-blob)) "Incorrect blob size")
      (is (= (aget (bc/blob->bytes (:blob stored-blob)) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (:created_at stored-blob))) "This blob doesn't have a created date"))))

(defn test-update-blob
  [service]
  (let [blob-id (b/store! service (byte-array [(byte 0) (byte 1) (byte 2)]))
        new-blob (byte-array [(byte 3)])
        _ (b/update! service blob-id new-blob)
        updated-blob (b/blob service blob-id)
        blob-meta (b/blob-metadata service blob-id)]

    (testing "update blob"
      (is (= (:id updated-blob) blob-id) "Different blob id from the updated")
      (is (not (nil? (:blob updated-blob))) "Blob updated incorrectly")
      (is (= (alength new-blob) (:size updated-blob)) "Incorrect blob size")
      (is (= (aget (bc/blob->bytes (:blob updated-blob)) 0) 3) "This blob doesn't seem like the one I stored")
      (is (not (nil? (:created_at updated-blob))) "This blob doesn't have a created date")
      (is (not (nil? (:updated_at updated-blob))) "This blob doesn't have a updated date")
      (is (= blob-meta (dissoc updated-blob :blob))))))

(defn test-delete-blob
  [service]
  (let [blob-id (b/store! service (byte-array [(byte 0) (byte 1) (byte 2)]))
        _ (b/del! service blob-id)
        deleted-blob (b/blob service blob-id)]
    (testing "update blob"
      (is (nil? deleted-blob) "Found blob after deletion"))))

