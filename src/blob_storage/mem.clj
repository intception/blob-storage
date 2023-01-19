(ns blob-storage.mem
  (:gen-class)
  (:require [blob-storage.api :as api :refer [BlobStorage]]
            [blob-storage.coerce :refer [coerce-blob blob->bytes]]
            [clojure.java.io :as io]))

(defrecord Mem [config]
  BlobStorage

  (init-schema! [service]
    (swap! config assoc :blobs {}))

  (drop-schema! [service]
    (api/init-schema! service))

  (store! [service blob]
    (let [id (str (java.util.UUID/randomUUID))
          [blob-size input-stream] (coerce-blob blob)]
      (swap! config
             #(assoc-in %
                        [:blobs id]
                        {:id id
                         :created_at (java.util.Date.)
                         :updated_at (java.util.Date.)
                         :size blob-size
                         :blob (blob->bytes input-stream blob-size)}))
      id))

  (store! [service blob {:keys [id tag] :as data}]
    (let [id (or id (str (java.util.UUID/randomUUID)))
          [blob-size input-stream] (coerce-blob blob)]
      (swap! config
             #(assoc-in %
                        [:blobs id]
                        {:id id
                         :created_at (java.util.Date.)
                         :updated_at (java.util.Date.)
                         :size blob-size
                         :tag tag
                         :blob (blob->bytes input-stream blob-size)}))
      id))

  (update! [service id blob]
    (let [[blob-size input-stream] (coerce-blob blob)]
      (swap! config
             #(update-in %
                         [:blobs id]
                         merge
                         {:updated_at (java.util.Date.)
                          :size blob-size
                          :blob (blob->bytes input-stream blob-size)}))))

  (del! [service id]
    (swap! config #(update % :blobs dissoc id)))

  (blob [service id]
    (when-let [record (get-in @config [:blobs id])]
      (update record :blob io/input-stream)))

  (blob-metadata [_ id]
    (when-let [record (get-in @config [:blobs id])]
      (dissoc record :blob))))

(defn make
  []
  (->Mem (atom {})))
