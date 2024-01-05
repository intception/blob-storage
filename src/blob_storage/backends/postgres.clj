(ns blob-storage.backends.postgres
  (:require [blob-storage.postgres.schema :as schema]
            [blob-storage.core.backend :refer [Backend]]
            [blob-storage.core.blob :as blob-impl]))

;; implement the backend protocol using functions in postgres.schema

(deftype Postgres [db]

  Backend
  (init-schema! [this]
    (schema/create-blobs-table! db))

  (drop-schema! [this]
    (schema/drop-blobs-table! db))

  (store! [this id [size blob] metadata]
    (schema/inup-blob! db [size blob] id metadata))

  (update! [this id [size blob]]
    (schema/update-blob! db id [size blob]))

  (delete! [this id]
    (schema/delete-blob! db id))

  (get [this id lazy?]
    (if lazy?
      (when-let [{:keys [id size tag created_at updated_at]} (schema/get-blob-metadata db id)]
        (blob-impl/make-lazy id size tag created_at updated_at {}
                             (fn [_]
                               (:blob (schema/get-blob db id)))))
      (when-let [{:keys [id size tag created_at updated_at blob file]} (schema/get-blob db id)]
        (if (bytes? blob)
          (blob-impl/make-from-bytes id size tag created_at updated_at {} blob)
          (when file
            (blob-impl/make-from-file id size tag created_at updated_at {} file)))))))

(defn make
  [db-spec]
  (Postgres. db-spec))
