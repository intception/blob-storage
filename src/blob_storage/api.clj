(ns blob-storage.api)

(defprotocol BlobBinary
  "Implemented by the :blob object returned by BlobStorage/blob. A blob
   may be stored in memory or in a temporary file on disk. Use these
   functions to work with it as it fits your needs."

  (^Boolean bytea? [this]
    "Whether this blob is loaded in memory as a byte array or not")

  (^"[B" get-bytes [this]
    "Returns the blob as a byte array. Has no cost if (bytea? this) is true.")

  (^java.io.InputStream get-stream [this]
    "Returns the blob as an input stream"))

(defprotocol BlobStorage
  (init-schema! [service]
    "Initializes schema")

  (drop-schema! [service]
    "Drops the schema")

  (store! [service blob] [service blob id]
    "Adds a new blob to the database. `blob` can be a file, a byte array or an input stream.")

  (update! [service id blob]
    "Updates a blob from the database. `blob` can be a file, a byte array or an input stream.")

  (del! [service id]
    "Delete a blob from the database")

  (blob [service id]
    "Retrieves a blob given its id.
     Returns a map of {:id, :size, :blob, :created-at, :updated-at}, where
     :blob is a BlobBinary object."))
