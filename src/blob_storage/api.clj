(ns blob-storage.api)

(defprotocol BlobStorage
  (init-schema! [service]
    "Initializes schema")

  (drop-schema! [service]
    "Drops the schema")

  (store! [service blob] [service blob {:keys [id tag]}]
    "Adds a new blob to the database. `blob` can be a file, a byte array or an input stream.")

  (update! [service id blob]
    "Updates a blob from the database. `blob` can be a file, a byte array or an input stream.")

  (del! [service id]
    "Delete a blob from the database")

  (blob [service id]
    "Retrieves a blob given its id. Returns a map of:
     - :id - the id of the blob (same as the id argument)
     - :tag - user defined tag associated with this blob
     - :size - the size of the blob in bytes
     - :created_at - the date when the blob was created
     - :updated_at - the date when the blob was last updated (if any)
     - :blob  - the blob itself (a java.io.InputStream object)")

  (blob-metadata [service id]
    "Retrieves metadata for a blob given its id (blob not included).
     Returns a map of:
     - :id - the id of the blob (same as the id argument
     - :tag - user defined tag associated with this blob)
     - :size - the size of the blob in bytes
     - :created_at - the date when the blob was created
     - :updated_at - the date when the blob was last updated (if any)"))
