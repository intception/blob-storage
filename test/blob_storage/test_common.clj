(ns blob-storage.test-common
  (:require [blob-storage.api :as b]
            [blob-storage.core :as bs]
            [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:import (java.io File)))


(defn stream->bytes
  [input-stream]
  (with-open [os (java.io.ByteArrayOutputStream.)]
    (io/copy input-stream os :buffer-size 4096)
    (.toByteArray os)))

(defn test-store-blob
  [service]
  (let [blob (byte-array [(byte 0) (byte 1) (byte 2)])
        blob-id (b/store! service blob)
        stored-blob (b/blob service blob-id)]
    (testing "store byte[] blob"
      (is (= (b/get-id stored-blob) blob-id) "Different blob id from the stored")
      (is (not (nil? (b/get-bytes stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (b/get-size stored-blob)) "Incorrect blob size")
      (is (= (alength blob) (alength (stream->bytes (b/open-input-stream stored-blob)))) "Incorrect stream blob size")
      (is (some? (b/get-bytes stored-blob)) "Blob is nil")
      (is (= (aget (b/get-bytes stored-blob) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (b/created-at stored-blob))) "This blob doesn't have a created date")))

  (let [file (File/createTempFile "test-blob-storage" ".bin")]
    (try
      (let [_ (->> (repeatedly 2000 #(+ 32 (rand-int 95)))
                   (mapv char)
                   (clojure.string/join)
                   (spit file))
            blob-id (b/store! service file)
            stored-blob (b/blob service blob-id)]
        (testing "store file blob"
          (is (= (b/get-id stored-blob) blob-id) "Different blob id from the stored")
          (is (not (nil? (b/get-bytes stored-blob))) "Blob stored incorrectly")
          (is (= (.length file) (b/get-size stored-blob)) "Incorrect blob size")
          (is (= (.length file) (alength (stream->bytes (b/open-input-stream stored-blob)))) "Incorrect stream blob size")
          (is (some? (b/get-bytes stored-blob)) "Blob is nil")
          (is (not (nil? (b/created-at stored-blob))) "This blob doesn't have a created date")))
      (finally
        (.delete file)))))

(defn test-store-blob-with-id
  [service]
  (let [blob-id "pan casero"
        blob (byte-array [(byte 0) (byte 1) (byte 2)])
        returned-blob-id (b/store! service blob {:id blob-id})
        stored-blob (b/blob service blob-id)]
    (testing "store blob"
      (is (= (b/get-id stored-blob) returned-blob-id blob-id) "Different blob id from the stored")
      (is (not (nil? (b/get-bytes stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (b/get-size stored-blob)) "Incorrect blob size")
      (is (= (aget (b/get-bytes stored-blob) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (b/created-at stored-blob))) "This blob doesn't have a created date"))))

(defn test-store-blob-with-tag
  [service]
  (let [tag "test-tag"
        blob (byte-array [(byte 0) (byte 1) (byte 2)])
        blob-id (b/store! service blob {:tag tag})
        stored-blob (b/blob service blob-id)]
    (testing "store blob"
      (is (= (b/get-id stored-blob) blob-id) "Different blob id from the stored")
      (is (= (b/get-tag stored-blob) tag) "Different tag from the stored")
      (is (not (nil? (b/get-bytes stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (b/get-size stored-blob)) "Incorrect blob size")
      (is (= (aget (b/get-bytes stored-blob) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (b/created-at stored-blob))) "This blob doesn't have a created date"))))

(defn test-store-blob-with-id-and-tag
  [service]
  (let [blob-id "pan casero"
        tag "test-tag"
        blob (byte-array [(byte 0) (byte 1) (byte 2)])
        returned-blob-id (b/store! service blob {:id blob-id :tag tag})
        stored-blob (b/blob service blob-id)]
    (testing "store blob"
      (is (= (b/get-id stored-blob) returned-blob-id blob-id) "Different blob id from the stored")
      (is (= (b/get-tag stored-blob) tag) "Different tag from the stored")
      (is (not (nil? (b/get-bytes stored-blob))) "Blob stored incorrectly")
      (is (= (alength blob) (b/get-size stored-blob)) "Incorrect blob size")
      (is (= (aget (b/get-bytes stored-blob) 2) 2) "This blob doesn't seem like the one I stored")
      (is (not (nil? (b/created-at stored-blob))) "This blob doesn't have a created date"))))

(defn test-update-blob
  [service]
  (let [blob-id (b/store! service (byte-array [(byte 0) (byte 1) (byte 2)]))
        new-blob (byte-array [(byte 3)])
        _ (b/update! service blob-id new-blob)
        updated-blob (b/blob service blob-id)
        blob-meta (b/blob service blob-id true)]

    (testing "update blob"
      (is (= (b/get-id updated-blob) blob-id) "Different blob id from the updated")
      (is (not (nil? (b/get-bytes updated-blob))) "Blob updated incorrectly")
      (is (= (alength new-blob) (b/get-size updated-blob)) "Incorrect blob size")
      (is (= (aget (b/get-bytes updated-blob) 0) 3) "This blob doesn't seem like the one I stored")
      (is (not (nil? (b/created-at updated-blob))) "This blob doesn't have a created date")
      (is (not (nil? (b/updated-at updated-blob))) "This blob doesn't have a updated date")
      (is (= blob-meta (dissoc updated-blob :blob))))))

(defn test-delete-blob
  [service]
  (let [blob-id (b/store! service (byte-array [(byte 0) (byte 1) (byte 2)]))
        _ (b/del! service blob-id)
        deleted-blob (b/blob service blob-id)]
    (testing "update blob"
      (is (nil? deleted-blob) "Found blob after deletion"))))


(defn test-local-cache
  [backend]
  (let [make-payload (fn [size]
                       (->> (repeatedly size #(+ 32 (rand-int 95)))
                            (mapv char)
                            (clojure.string/join)))
        counters (atom {:hits 0
                        :misses 0})
        ;; work on a new service
        service (bs/make backend
                         {:cache-lru-threshold 3
                          :cache-ttl-threshold 1000
                          :on-cache-hit (fn []
                                          (swap! counters update-in [:hits] inc))
                          :on-cache-miss (fn []
                                           (swap! counters update-in [:misses] inc))})
        _ (b/drop-schema! service)
        _ (b/init-schema! service)
        _ (b/store! service (.getBytes (str "blob1\n" (make-payload 100))) {:id "1"})
        _ (b/store! service (.getBytes (str "blob2\n" (make-payload 500))) {:id "2"})
        _ (b/store! service (.getBytes (str "blob3\n" (make-payload 1000))) {:id "3"})
        _ (b/store! service (.getBytes (str "blob4\n" (make-payload 2000))) {:id "4"})]

    (let [blob (b/blob service "1")]
      (is (= "1" (b/get-id blob)))
      (is (= 106 (b/get-size blob)))
      (is (= 106 (alength (b/get-bytes blob))))
      (is (= 106 (.length (b/get-file blob))))

      (is (= 1 (count @(:cache service))))
      (is (= 0 (:hits @counters)))
      (is (= 1 (:misses @counters))))

    (is (= "1" (b/get-id (b/blob service "1"))))
    (is (= 1 (count @(:cache service))))
    (do
      (is (= 1 (:hits @counters)))
      (is (= 1 (:misses @counters))))

    (let [blob (b/blob service "2")]
      (is (= "2" (b/get-id blob)))
      (is (= 506 (b/get-size blob)))
      (is (= 506 (alength (b/get-bytes blob))))
      (is (= 506 (.length (b/get-file blob))))

      (is (= 2 (count @(:cache service))))
      (is (= 1 (:hits @counters)))
      (is (= 2 (:misses @counters))))

    (is (= "2" (b/get-id (b/blob service "2"))))
    (is (= 2 (count @(:cache service))))
    (do
      (is (= 2 (:hits @counters)))
      (is (= 2 (:misses @counters))))

    (let [blob (b/blob service "3")]
      (is (= "3" (b/get-id blob)))
      (is (= 1006 (b/get-size blob)))
      (is (= 1006 (alength (b/get-bytes blob))))
      (is (= 1006 (.length (b/get-file blob))))
      (is (= 3 (count @(:cache service))))
      (is (= 2 (:hits @counters)))
      (is (= 3 (:misses @counters))))

    ;; getting a fourth element will evict the oldest one

    (let [blob (b/blob service "4")]
      (is (= "4" (b/get-id blob)))
      (is (= 3 (count @(:cache service))))
      (is (= 2 (:hits @counters)))
      (is (= 4 (:misses @counters))))

    (is (= "1" (b/get-id (b/blob service "1"))))
    (do
      (is (= 2 (:hits @counters)))
      (is (= 5 (:misses @counters))))

    (is (= "3" (b/get-id (b/blob service "3"))))
    (do
      (is (= 3 (:hits @counters)))
      (is (= 5 (:misses @counters))))

    (Thread/sleep 600)

    (let [blob-id (b/store! service (.getBytes (str "blob5\n" (make-payload 50000))))
          blob (b/blob service blob-id)]
      (is (= blob-id (b/get-id blob)))
      (is (= 50006 (b/get-size blob)))
      (is (= 50006 (alength (b/get-bytes blob))))
      (is (= 50006 (.length (b/get-file blob))))
      (is (= 3 (:hits @counters)))
      (is (= 6 (:misses @counters)))) ; this was a miss

    (is (= "1" (b/get-id (b/blob service "1"))))
    (do
      (is (= 4 (:hits @counters))) ; this was a hit, 1 still in cache
      (is (= 6 (:misses @counters))))

    ;; after this sleep all entries but blob5 should've been evicted
    (Thread/sleep 600)

    (is (= "1" (b/get-id (b/blob service "1"))))
    (do
      (is (= 4 (:hits @counters)))
      (is (= 7 (:misses @counters))) ; miss due to ttl
      )))
