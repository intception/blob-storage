(ns blob-storage.postgres.schema
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb]
            [pallet.thread-expr :as th]
            [blob-storage.api :refer [BlobBinary]]
            [blob-storage.coerce :refer [input-stream->byte-array]])
  (:use sqlingvo.core)
  (:import (org.postgresql PGConnection)
           (org.postgresql.largeobject LargeObjectManager)
           (java.io File FileInputStream FileOutputStream ByteArrayInputStream)))

(def sqdb (sqdb/postgresql))

(deftype Blob [bytes file]
  BlobBinary

  (bytea? [_]
    (some? bytes))

  (get-bytes [_]
    (if (some? bytes)
      bytes
      (let [result (byte-array (.length file))]
        (with-open [stream (FileInputStream. ^File file)]
          (.read stream result))
        result)))

  (get-stream [_]
    (if (some? bytes)
      (ByteArrayInputStream. bytes)
      (FileInputStream. ^File file))))

(def large-object-threshold
  "Value is set via blob-storage.large-object-threshold system property; defaults to
  50 MB; uses a delay so property can be set from code after namespace is loaded but
  before any use of the storage functions."
  (delay (or (when-let [prop (System/getProperty "blob-storage.large-object-threshold")]
               (Long/parseLong prop))
             (* 50 1024 1024))))

(defn- copy-streams!
  "Like clojure.java.io/copy but works with org.postgresql.largeobject.LargeObject."
  [input output]
  (let [buffer (byte-array 1024)]
    (loop []
      (let [size (.read input buffer 0 1024)]
        (when (pos? size)
          (do (.write output buffer 0 size)
              (recur)))))))

(defn- large-object->file
  ^File
  [^java.sql.Connection conn oid]
  (let [^PGConnection pg-conn (.unwrap conn org.postgresql.PGConnection)
        ^LargeObjectManager lo-mgr (.getLargeObjectAPI pg-conn)
        obj (.open lo-mgr ^long oid LargeObjectManager/READ)
        file (File/createTempFile "blob-storage" ".bin")]
    (with-open [stream (FileOutputStream. file)]
      (copy-streams! obj stream))
    file))

(defn- execute-upsert-transaction!
  [db [blob-size input-stream] make-upsert-sql]
  (if (< blob-size @large-object-threshold)
    (let [sql (make-upsert-sql {:oid nil ; FIXME: Does not delete the old large object
                                :blob (input-stream->byte-array input-stream blob-size)
                                :size blob-size
                                :updated-at '(now)})]
      (j/execute! db sql))
    (j/with-db-transaction [t-conn db]
      (let [conn (j/get-connection t-conn)
            ^PGConnection pg-conn (.unwrap conn org.postgresql.PGConnection)
            ^LargeObjectManager lo-mgr (.getLargeObjectAPI pg-conn)
            oid (.createLO lo-mgr LargeObjectManager/READWRITE)]
        ;; upload large object
        (with-open [obj (.open lo-mgr oid LargeObjectManager/WRITE)]
          (copy-streams! input-stream obj))
        ;; update the table in the same transaction
        (let [sql (make-upsert-sql {:oid oid
                                    :blob nil
                                    :size blob-size
                                    :updated-at '(now)})
              stmt (j/prepare-statement conn (first sql))]
          (j/execute! db (cons stmt (rest sql))))))))

(defn get-blob
  "Retrieves a blob from database, nil if blob does not exists"
  [db id]
  (j/with-db-transaction [t-conn db {:read-only? true}]
    (let [conn (j/get-connection t-conn)
          stmt (j/prepare-statement conn
                                    "SELECT * FROM blobs WHERE id = ?")
          row (first (j/query db [stmt id]))]
      (some-> row
              (th/if-> (:oid row)
                (assoc :blob
                       (->Blob nil (large-object->file conn (:oid row))))
                (clojure.core/update
                  :blob
                  #(->Blob % nil)))
              ;; internal implementation detail, no need to expose it
              (dissoc :oid)))))

(defn create-blobs-table!
  "Creates the blobs table if not exists"
  [db]
  (j/execute! db
              (sql
                (create-table sqdb :blobs
                              (if-not-exists true)
                              (column :id :varchar :length 150 :primary-key? true)
                              (column :blob :bytea)
                              (column :oid :oid)
                              (column :size :bigint :not-null? true)
                              (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
                              (column :updated-at :timestamp-with-time-zone)))))

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
  [db [blob-size input-stream :as blob]]
  (let [id (str (java.util.UUID/randomUUID))]
    (when (execute-upsert-transaction! db
                                       blob
                                       (fn [upserts]
                                         (sql (insert sqdb
                                                      :blobs
                                                      []
                                                      (values (reduce (fn [m [k v]]
                                                                        ; remove nil values
                                                                        (if v (assoc m k v) m))
                                                                      {:id id}
                                                                      (dissoc upserts :updated-at)))))))
      id)))

(defn inup-blob!
  "Inserts the new blob to the database using the specified id.

  Returns the id generated"
  [db [blob-size input-stream :as blob] id]
  (execute-upsert-transaction! db
                               blob
                               (fn [upserts]
                                 (sql
                                   (with sqdb
                                         [:upsert (update sqdb
                                                          :blobs
                                                          upserts
                                                          (where `(= :id ~id))
                                                          (returning *))]
                                         (insert sqdb :blobs (vec (cons :id (filter #(get upserts %) (keys upserts))))
                                                 (select sqdb (vec (cons id (filter identity (vals upserts)))))
                                                 (where `(not-exists ~(select sqdb [*]
                                                                              (from :upsert)))))))))
  id)

(defn update-blob!
  "Updates a blob from the database"
  [db id [blob-size input-stream :as blob]]
  (execute-upsert-transaction! db
                               blob
                               (fn [upserts]
                                 (sql (update sqdb :blobs upserts
                                              (where `(= :id ~id)))))))

(defn delete-blob!
  "Deletes a blob from the database"
  [db id]
  (j/with-db-transaction [t-conn db]
    (let [conn (j/get-connection t-conn)
          stmt (j/prepare-statement conn
                                    "SELECT oid FROM blobs WHERE id = ?")
          row (first (j/query db [stmt id]))]
      (when row
        (when-some [oid (:oid row)]
          (let [^PGConnection pg-conn (.unwrap conn org.postgresql.PGConnection)
                ^LargeObjectManager lo-mgr (.getLargeObjectAPI pg-conn)]
            (.delete lo-mgr oid)))
        (let [stmt (j/prepare-statement conn
                                        "DELETE FROM blobs WHERE id = ?")]
          (j/execute! db [stmt id]))))))
