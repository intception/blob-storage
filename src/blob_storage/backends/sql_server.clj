(ns blob-storage.backends.sql-server
  (:require [blob-storage.sql-server.schema :as schema]
            [blob-storage.core.backend :refer [Backend]]
            [blob-storage.core.blob :as blob-impl]))

(deftype SqlServer [db]

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
      (when-let [{:keys [id size tag created_at updated_at blob]} (schema/get-blob db id)]
        (blob-impl/make-from-bytes id size tag created_at updated_at {} blob)))))

(defn make
  [db-spec]
  (SqlServer. db-spec))
