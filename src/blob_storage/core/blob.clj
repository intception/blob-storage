(ns blob-storage.core.blob
  (:require [blob-storage.api :as api]
            [clojure.java.io :as io])
  (:import (java.io File)))

(defn file->bytes
  [^File file]
  (when file
    (let [result (byte-array (.length file))]
      (with-open [stream (io/input-stream file)]
        ;; ok with the underlying implementation of readBytes
        (.read stream result))
      result)))

(defn bytes->file
  ([^"[B" bytes]
   (when bytes
     (bytes->file bytes (File/createTempFile "blob-storage" ".bin"))))
  ([^"[B" bytes ^File file]
   (when bytes
     (with-open [stream (io/output-stream file)]
       (.write stream bytes))
     file)))

(defn wrap-get-blob-fn
  [get-blob-fn]
  (fn [state id]
    (let [blob (get-blob-fn id)]
      (swap! state assoc
             (if (instance? File blob) :file :bytes) blob
             :get-blob-fn nil)
      blob)))

(defrecord BlobImpl [id size tag created-at updated-at metadata state]

  api/Blob

  (get-id [_]
    id)

  (open-input-stream [this]
    (if-let [bytes (get @state :bytes)]
      (io/input-stream bytes)
      (when-let [file (get @state :file)]
        (io/input-stream file))))

  (get-bytes [this]
    (if-let [bytes (get @state :bytes)]
      bytes
      (when-let [bytes (file->bytes (get @state :file))]
        (swap! state assoc :bytes bytes)
        bytes)))

  (get-file [this]
    (if-let [file (get @state :file)]
      file
      (when-let [file (bytes->file (get @state :bytes))]
        (swap! state assoc :file file)
        file)))

  (get-size [this]
    size)

  (created-at [this]
    created-at)

  (updated-at [this]
    updated-at)

  (get-tag [this]
    tag)

  (get-metadata [this]
    (merge
      metadata
      {:id id
       :tag tag
       :size size
       :created_at created-at
       :updated_at updated-at})))

(defn free-storage
  [blob]
  (when-let [^File file (get @(:state blob) :file)]
    (.delete file)
    (swap! (:state blob) dissoc :file)))

(defn make-from-file [id size tag created-at updated-at metadata file]
  (BlobImpl. id size tag created-at updated-at metadata (atom {:file file})))

(defn make-from-bytes [id size tag created-at updated-at metadata bytes]
  (BlobImpl. id size tag created-at updated-at metadata (atom {:bytes bytes})))

(defn make-lazy [id size tag created-at updated-at metadata get-blob-fn]
  (BlobImpl. id size tag created-at updated-at metadata
             (atom {:get-blob-fn (wrap-get-blob-fn get-blob-fn)})))
