(ns blob-storage.api)


(defprotocol BlobStorage
  (init-schema! [service]
    "Initializes schema")

  (drop-schema! [service]
    "Drops the schema")

  (store! [service blob] [service blob id]
    "Adds a new blob to the database")

  (update! [service id blob]
    "Updates a blob from the database")

  (del! [service id]
    "Delete a blob from the database")

  (blob [service id]
    "Retrieves a blob given its id"))
