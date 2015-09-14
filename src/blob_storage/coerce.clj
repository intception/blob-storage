(ns blob-storage.coerce)


(defmulti coerce-blob (fn [input] (type input)))

(defmethod coerce-blob java.io.BufferedInputStream
  [input]
  (with-open [out (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy input out)
    (.toByteArray out)))

(defmethod coerce-blob :default [input] input)