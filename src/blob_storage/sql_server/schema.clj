(ns blob-storage.sql-server.schema
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb]
            [blob-storage.api :refer [BlobBinary]]
            [blob-storage.coerce :refer [input-stream->byte-array]])
  (:use sqlingvo.core))

(def sqdb (sqdb/sqlserver))

(deftype Blob [^"[B" bytes]
  BlobBinary

  (bytea? [_]
    true)

  (get-bytes [_]
    bytes)

  (get-stream [_]
    (java.io.ByteArrayInputStream. bytes)))

(defn get-blob
  "Retrieves a blob from database, nil if blob does not exists"
  [db id]
  (if-some [row (first
                  (j/query db
                           (sql
                             (select sqdb [*]
                                     (from :blobs)
                                     (where `(= :id ~id))))))]
    (clojure.core/update row
                         :blob
                         #(->Blob %))))

(defn blobs-table-exists?
  [db]
  (->> (sql
         (select sqdb [:name]
                 (from :SysObjects)
                 (where '(= :xType "U"))
                 (where '(like :name "%blobs%") :and)))
       (j/query db)
       (first)))


(defn create-blobs-table!
  "Creates the blobs table if not exists"
  [db]
  (when-not (blobs-table-exists? db)
    (j/execute! db
                (sql
                  (create-table sqdb :blobs
                                (column :id :varchar :length 150 :primary-key? true)
                                (column :blob (keyword "varbinary(max)") :not-null? true)
                                (column :size :bigint :not-null? true)
                                (column :created-at :datetime :not-null? true)
                                (column :updated-at :datetime))))))

(defn drop-blobs-table!
  "Drop the blobs table if exists"
  [db]
  (j/execute! db
              (sql
                (drop-table sqdb [:blobs]))))

(defn store-blob!
  "Inserts the new blob to the database.
  The blob id is created internally.

  Returns the id generated"
  [db [blob-size input-stream]]
  (let [id (str (java.util.UUID/randomUUID))]
    (when (j/execute! db
                      (sql (insert sqdb :blobs []
                                   (values {:id id
                                            :blob (input-stream->byte-array input-stream blob-size)
                                            :created-at (java.util.Date.)
                                            :size blob-size}))))
      id)))


(def upsert-query-string ""
  "DECLARE @id VARCHAR(150), @blob VARBINARY(max), @size BIGINT, @created_at datetime;
   SELECT @id = ?, @blob = ?, @size = ?, @created_at = ?;

   IF EXISTS (select * FROM blobs WITH (updlock,serializable) where id=@id)
     BEGIN
          UPDATE blobs WITH (updlock)
          SET id=@id, blob=@blob, size=@size, created_at=@created_at
          WHERE id=@id
     END
   ELSE
     BEGIN
          INSERT blobs
          ([id], [blob], [size], [created_at]) VALUES (@id, @blob, @size, @created_at)
     END")

(defn inup-blob!
  "Inserts the new blob to the database using the specified id.
  The blob id is created internally.

  Returns the id generated"
  [db [blob-size input-stream] id]
  (j/execute! db [upsert-query-string
                  id
                  (input-stream->byte-array input-stream blob-size)
                  blob-size
                  (java.util.Date.)]))

(defn update-blob!
  "Updates a blob from the database"
  [db id [blob-size input-stream]]
  (j/execute! db
              (sql (update sqdb :blobs {:updated-at (java.util.Date.)
                                        :blob (input-stream->byte-array input-stream blob-size)
                                        :size blob-size}
                           (where `(= :id ~id))))))

(defn delete-blob!
  "Deletes a blob from the database"
  [db id]
  (j/execute! db
              (sql (delete sqdb :blobs
                           (where `(= :id ~id))))))
