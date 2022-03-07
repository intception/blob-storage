(ns blob-storage.postgres
  (:gen-class)
  (:require [blob-storage.api :as api :refer [BlobStorage]]
            [blob-storage.coerce :refer [coerce-blob]]
            [blob-storage.postgres.schema :as schema]))


(defrecord Postgres [config]
  BlobStorage

  (init-schema! [service]
    (schema/create-blobs-table! config))

  (drop-schema! [service]
    (schema/drop-blobs-table! config))

  (store! [service blob]
    (schema/store-blob! config (coerce-blob blob)))

  (store! [service blob id]
    (schema/inup-blob! config (coerce-blob blob) id))

  (update! [service id blob]
    (schema/update-blob! config id (coerce-blob blob)))

  (del! [service id]
    (schema/delete-blob! config id))

  (blob [service id]
    (schema/get-blob config id))

  (blob-metadata [service id]
    (schema/get-blob-metadata config id)))

(defn make
  "Creates a new PostgreSQL service"
  [db-spec]
  (Postgres. db-spec))
