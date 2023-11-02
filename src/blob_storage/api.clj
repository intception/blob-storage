(ns blob-storage.api)

(defprotocol Blob

  (get-id [blob]
    "Returns the id of the blob")

  (open-input-stream [blob]
    "Returns the blob as an input stream")

  (get-bytes [blob]
    "Returns the blob as a byte array")

  (get-file [blob]
    "Returns the blob as a file")

  (get-size [blob]
    "Returns the size of the blob in bytes")

  (created-at [blob]
    "Returns the date when the blob was created")

  (updated-at [blob]
    "Returns the date when the blob was last updated (if any)")

  (get-tag [blob]
    "Returns the tag associated with the blob (if any)")

  (get-metadata [blob]
    "Returns the metadata of the blob as a map of:
     - :size - the size of the blob in bytes
     - :created_at - the date when the blob was created
     - :updated_at - the date when the blob was last updated (if any)
     - Any other user-provided metadata"))


(defprotocol BlobStorage
  (init-schema! [this]
    "Initializes schema")

  (drop-schema! [this]
    "Drops the schema")

  (store! [this blob] [this blob {:keys [id tag metadata]}]
    "Adds a new blob to the database. `blob` can be a file, a byte array or an input stream.")

  (update! [this id blob]
    "Updates a blob from the database. `blob` can be a file, a byte array or an input stream.")

  (del! [service id]
    "Delete a blob from the database")

  (blob [this id] [this id {:keys [lazy? no-cache?]}]
    "Retrieves the blob with the given id.
    If `lazy?` si true, it will delay loading the blob itself until it is accessed.
    If `no-cache?` is true, it will ignore the cache and load the blob from the database."))


;; prevent IllegalArgumentException when blob is nil
;; this is done mainly because of backwards compatibility
(extend-protocol Blob
  nil
  (get-id [_] nil)
  (open-input-stream [_] nil)
  (get-bytes [_] nil)
  (get-file [_] nil)
  (get-size [_] nil)
  (created-at [_] nil)
  (updated-at [_] nil)
  (get-tag [_] nil)
  (get-metadata [_] nil))
