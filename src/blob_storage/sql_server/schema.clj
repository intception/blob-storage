(ns blob-storage.sql-server.schema
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb])
  (:use sqlingvo.core))


(def sqdb (sqdb/sqlserver))

(defn get-blob
  "Retrieves a blob from database, nil if blob does not exists"
  [db id]
  (first
    (j/query db
             (sql
               (select sqdb [*]
                       (from :blobs)
                       (where `(= :id ~id)))))))

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
                                (column :id :varchar :length 40 :primary-key? true)
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
  [db #^bytes blob]
  (let [id (str (java.util.UUID/randomUUID))]
    (when (j/execute! db
                      (sql (insert sqdb :blobs []
                                   (values {:id id
                                            :blob blob
                                            :created-at (java.util.Date.)
                                            :size (alength blob)}))))
      id)))

(defn update-blob!
  "Updates a blob from the database"
  [db id #^bytes blob]
  (j/execute! db
              (sql (update sqdb :blobs {:updated-at (java.util.Date.)
                                        :blob blob
                                        :size (alength blob)}
                           (where `(= :id ~id))))))

(defn delete-blob!
  "Deletes a blob from the database"
  [db id]
  (j/execute! db
              (sql (delete sqdb :blobs
                           (where `(= :id ~id))))))
