(ns blob-storage.coerce)

(defmulti coerce-blob
  "Coerces the blob to a vector of [size input-stream]"
  ^java.io.File
  type)

(defmethod coerce-blob (class (byte-array 0))
  [^"[B" input]
  [(alength input) (java.io.ByteArrayInputStream. input)])

(defmethod coerce-blob java.io.File
  [^java.io.File input]
  [(.length input) (java.io.FileInputStream. input)])

;; this one will copy the input stream to a
;; temporary file so avoid whenever possible
(defmethod coerce-blob java.io.InputStream
  [^java.io.InputStream input]
  (let [temp-file (java.io.File/createTempFile "blob-storage" ".bin")]
    (clojure.java.io/copy input temp-file)
    (coerce-blob temp-file)))

(defn input-stream->byte-array
  [^java.io.InputStream input-stream size]
  (let [buff (byte-array size)]
    (.read input-stream buff)
    buff))
