(ns blob-storage.postgres.schema
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb])
  (:use sqlingvo.core))


(def sqdb (sqdb/postgresql))

(defn get-blob
  "Retrieves a blob from database, nil if blob does not exists"
  [db id]
  (first
    (j/query db
             (sql
               (select sqdb [*]
                       (from :blobs)
                       (where `(= :id ~id)))))))


(defn create-blobs-table!
  "Creates the blobs table if not exists"
  [db]
  (j/execute! db
              (sql
                (create-table sqdb :blobs
                              (if-not-exists true)
                              (column :id :varchar :length 40 :primary-key? true)
                              (column :blob :bytea :not-null? true)
                              (column :size :bigint :not-null? true)
                              (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
                              (column :updated-at :timestamp-with-time-zone)))))

(defn create-blobs-index!
  [db]
  (j/execute! db ["CREATE INDEX blobs_id_index ON blobs (id);"]))

(defn drop-blobs-index!
  [db]
  (j/execute! db ["DROP INDEX IF EXISTS blobs_id_index;"]))


(defn drop-blobs-table!
  "Drop the blobs table if exists"
  [db]
  (j/execute! db
              (sql
                (drop-table sqdb [:blobs]
                            (if-exists true)))))

(defn store-blob!
  "Inserts the new blob to the database.
  The blob id is created internally.

  Returns the id generated"
  [db #^bytes blob]
  (let [id (str (java.util.UUID/randomUUID))]
    (when (j/execute! db
                      (sql (insert sqdb :blobs []
                                   (values {:id id
                                            :blob blob
                                            :size (alength blob)}))))
      id)))

(defn update-blob!
  "Updates a blob from the database"
  [db id #^bytes blob]
  (j/execute! db
              (sql (update sqdb :blobs {:updated-at '(now)
                                        :blob blob
                                        :size (alength blob)}
                           (where `(= :id ~id))))))


(defn delete-blob!
  "Deletes a blob from the database"
  [db id]
  (j/execute! db
              (sql (delete sqdb :blobs
                           (where `(= :id ~id))))))