(ns blob-storage.core
  (:gen-class)
  (:require [blob-storage.api :as api :refer [BlobStorage]]
            [blob-storage.core.backend :as backend]
            [blob-storage.core.blob :as blob-impl]
            [blob-storage.core.cache :as cache]
            [blob-storage.core.coerce :refer [coerce-blob]]))

(declare handle-evicted-entries)

(defrecord BlobStorageImpl [backend cache cache-blob-size-threshold
                            on-cache-hit
                            on-cache-miss]

  BlobStorage

  (init-schema! [this]
    (backend/init-schema! backend))

  (drop-schema! [this]
    (backend/drop-schema! backend))

  (store! [this blob]
    (api/store! this blob {}))

  (store! [this blob {:keys [id tag metadata] :as data}]
    (let [id (or id (str (java.util.UUID/randomUUID)))]
      (backend/store! backend id (coerce-blob blob) (dissoc data :id))
      id))

  (update! [this id blob]
    (backend/update! backend id (coerce-blob blob))
    (when-let [evicted-blob (cache/evict cache id)]
      (blob-impl/free-storage evicted-blob))
    id)

  (del! [this id]
    (backend/delete! backend id)
    (when-let [evicted-blob (cache/evict cache id)]
      (blob-impl/free-storage evicted-blob)))

  (blob [this id]
    (api/blob this id {}))

  (blob [this id {:keys [lazy? no-cache?]}]
    ;; the cache is enabled by default so the user is not forced to specify it
    ;; explicitly. we negate the no-cache? flag to make it more intuitive
    ;; and avoid the double negation effect
    (let [use-cache? (not no-cache?)]
      (if-let [blob (and use-cache? (cache/get cache id))]
        (do
          (on-cache-hit id (api/get-size blob))
          blob)
        (when-let [blob (backend/get backend id (boolean lazy?))]
          (when use-cache?
            (on-cache-miss id)
            (when (< (api/get-size blob) cache-blob-size-threshold)
              (when-let [evicted-blobs (seq (cache/store cache id blob))]
                (doseq [blob evicted-blobs]
                  (blob-impl/free-storage blob)))))
          blob)))))

(defn make
  [backend {:keys [cache-lru-threshold
                   cache-ttl-threshold
                   cache-blob-size-threshold
                   on-cache-hit
                   on-cache-miss]
            :or {cache-lru-threshold 500
                 cache-ttl-threshold (* 60 60 1000)
                 cache-blob-size-threshold (* 10 1024 1024)}}]
  (->BlobStorageImpl backend
                     (cache/make cache-lru-threshold cache-ttl-threshold)
                     cache-blob-size-threshold
                     (or on-cache-hit (constantly true))
                     (or on-cache-miss (constantly true))))
