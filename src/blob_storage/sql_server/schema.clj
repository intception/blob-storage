(ns blob-storage.sql-server.schema
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as j]
            [clojure.java.io :as io]
            [pallet.thread-expr :as th]
            [sqlingvo.db :as sqdb]
            [blob-storage.core.coerce :as bc])
  (:use sqlingvo.core))

(def sqdb (sqdb/sqlserver))

(defn get-blob
  "Retrieves a blob from database, nil if blob does not exists"
  [db id]
  (when-let [row (first
                   (j/query db
                            (sql
                              (select sqdb [*]
                                      (from :blobs)
                                      (where `(= :id ~id))))))]
    row))

(defn get-blob-metadata
  "Retrieves blob metadata from database, nil if blob does not exists"
  [db id]
  (->> (sql
         (select sqdb [:id :tag :size :created-at :updated-at]
                 (from :blobs)
                 (where `(= :id ~id))))
       (j/query db)
       first))

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
    (j/with-db-transaction [t-conn db]
      (j/execute! db
                  (sql
                    (create-table sqdb :blobs
                                  (column :id :varchar :length 150 :primary-key? true)
                                  (column :tag :varchar :length 32)
                                  (column :blob (keyword "varbinary(max)") :not-null? true)
                                  (column :size :bigint :not-null? true)
                                  (column :created-at :datetime :not-null? true)
                                  (column :updated-at :datetime))))
      (j/execute! db "CREATE INDEX blobs_tag_idx ON blobs (tag)"))))

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
  [db [blob-size blob] {:keys [tag]}]
  (let [id (str (java.util.UUID/randomUUID))]
    (when (j/execute! db
                      (sql (insert sqdb :blobs []
                                   (values (-> {:id id
                                                :blob (bc/blob->bytes blob)
                                                :created-at (java.util.Date.)
                                                :size blob-size}
                                               (th/when-> tag
                                                 (assoc :tag tag)))))))
      id)))


(def upsert-query-string ""
  "DECLARE @id VARCHAR(150), @tag VARCHAR(32), @blob VARBINARY(max), @size BIGINT, @created_at datetime;
   SELECT @id = ?, @tag = ?, @blob = ?, @size = ?, @created_at = ?;

   IF EXISTS (select * FROM blobs WITH (updlock,serializable) where id=@id)
     BEGIN
          UPDATE blobs WITH (updlock)
          SET blob=@blob, size=@size, updated_at=@created_at
          WHERE id=@id
     END
   ELSE
     BEGIN
          INSERT blobs
          ([id], [tag], [blob], [size], [created_at]) VALUES (@id, @tag, @blob, @size, @created_at)
     END")

(defn inup-blob!
  "Inserts the new blob to the database using the specified id.
  The blob id is created internally.

  Returns the id generated"
  [db [blob-size blob] id {:keys [tag]}]
  (j/execute! db [upsert-query-string
                  id
                  tag
                  (bc/blob->bytes blob blob-size)
                  blob-size
                  (java.util.Date.)])
  id)

(defn update-blob!
  "Updates a blob from the database"
  [db id [blob-size blob]]
  (j/execute! db
              (sql (update sqdb :blobs {:updated-at (java.util.Date.)
                                        :blob (bc/blob->bytes blob blob-size)
                                        :size blob-size}
                           (where `(= :id ~id))))))

(defn delete-blob!
  "Deletes a blob from the database"
  [db id]
  (j/execute! db
              (sql (delete sqdb :blobs
                           (where `(= :id ~id))))))
