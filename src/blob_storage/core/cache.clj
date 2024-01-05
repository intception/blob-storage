(ns blob-storage.core.cache
  (:refer-clojure :exclude [get])
  (:require [blob-storage.core.blob :as blobs]
            [clojure.core.async :as async]
            [clojure.core.cache]))

(defn make
  "Creates a new cache with the given LRU and TTL thresholds. Returns an atom."
  [lru-threshold ttl-threshold]
  (-> {}
      (clojure.core.cache/lru-cache-factory :threshold lru-threshold)
      (clojure.core.cache/ttl-cache-factory :ttl ttl-threshold)
      atom))

(defn get
  "Returns the blob with the given id, if it is in the cache, otherwise nil.
  If the blob is in the cache, it is promoted to the front of the LRU queue."
  [cache id]
  (clojure.core.cache/lookup
    (swap! cache
           #(if (clojure.core.cache/has? % id)
              (clojure.core.cache/hit % id)
              %))
    id))

(defn store
  "Stores the given blob in the cache, if it is not already there.
  It may evict some entries if the cache is full, in that case it returns
  the evicted blobs."
  [cache id blob]
  (let [[old new] (swap-vals! cache
                              ;; the check is to account for the case where the blob is
                              ;; fetched from multiple threads at the same time
                              #(if-not (clojure.core.cache/has? % id)
                                 (clojure.core.cache/miss % id blob)
                                 %))
        evicted (when (not= old new)
                  (clojure.set/difference (set (keys old)) (set (keys new))))
        ;; `old` cannot be a cache because it contains expired values
        old (into {} old)]
    (->> evicted
         (mapv #(clojure.core/get old %)))))

(defn evict
  "Evicts the blob with the given id from the cache, if it is there."
  [cache id]
  (let [[old] (swap-vals! cache clojure.core.cache/evict id)]
    (clojure.core/get (into {} old) id)))
