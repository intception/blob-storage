(ns blob-storage.api)

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
    "Retrieves a blob given its id. Returns a map of:
     - :id - the id of the blob (same as the id argument)
     - :size - the size of the blob in bytes
     - :created-at - the date when the blob was created
     - :updated-at - the date when the blob was last updated (if any)
     - :blob  - the blob itself (a java.io.InputStream object)"))
