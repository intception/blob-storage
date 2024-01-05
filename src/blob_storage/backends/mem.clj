(ns blob-storage.backends.mem
  (:require [blob-storage.core.coerce :refer [blob->bytes]]
            [blob-storage.core.backend :refer [Backend]]
            [blob-storage.core.blob :as blob-impl]))

(defrecord Mem [config]

  Backend
  (init-schema! [service]
    (swap! config assoc :blobs {})
    true)

  (drop-schema! [service]
    (swap! config assoc :blobs {})
    true)

  (store! [this id [blob-size input-stream] {:keys [tag metadata]}]
    (swap! config
           #(assoc-in %
                      [:blobs id]
                      {:id id
                       :tag tag
                       :metadata metadata
                       :created_at (java.util.Date.)
                       :updated_at (java.util.Date.)
                       :size blob-size
                       :blob (blob->bytes input-stream blob-size)}))
    id)

  (update! [this id [blob-size input-stream]]
    (swap! config
           #(update-in %
                       [:blobs id]
                       merge
                       {:updated_at (java.util.Date.)
                        :size blob-size
                        :blob (blob->bytes input-stream blob-size)}))
    id)

  (delete! [this id]
    (swap! config #(update % :blobs dissoc id))
    nil)

  (get [this id lazy?]
    (when-let [record (get-in @config [:blobs id])]
      (blob-impl/make-from-bytes id
                                 (:size record)
                                 (:tag record)
                                 (:created_at record)
                                 (:updated_at record)
                                 (:metadata record)
                                 (:blob record)))))

(defn make
  []
  (->Mem (atom {})))
