(ns blob-storage.mem
  (:gen-class)
  (:require [blob-storage.api :as api :refer [BlobStorage]]
            [blob-storage.coerce :refer [coerce-blob]]
            [blob-storage.postgres.schema :as schema]))


(defrecord Mem [config]
  BlobStorage

  (init-schema! [service]
    (swap! config assoc :blobs {}))

  (drop-schema! [service]
    (api/init-schema! service))

  (store! [service blob]
    (let [id (str (java.util.UUID/randomUUID))]
      (swap! config #(assoc-in % [:blobs id]
                               {:id id
                                :updated-at (java.util.Date.)
                                :size (alength blob)
                                :blob (coerce-blob blob)}))
      id))

  (update! [service id blob]
    (swap! config #(assoc-in % [:blobs id]
                             {:id id
                              :updated-at (java.util.Date.)
                              :size (alength blob)
                              :blob (coerce-blob blob)})))

  (del! [service id]
    (swap! config #(assoc-in % [:blobs id] nil)))

  (blob [service id]
    (get (:blobs @config) id)))

(defn make
  []
  (->Mem (atom {})))
