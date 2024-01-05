(ns blob-storage.core.backend
  (:refer-clojure :exclude [get]))

(defprotocol Backend
  (init-schema! [this])
  (drop-schema! [this])
  (store! [this id blob metadata])
  (update! [this id blob])
  (delete! [this id])
  (get [this id lazy?]))
