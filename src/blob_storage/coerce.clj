(ns blob-storage.coerce
  (:require [clojure.java.io :as io])
  (:import (java.io File InputStream)))

;; from clojure.java.io
(def
  ^{:doc "Type object for a Java primitive byte array."
    :private true}
  byte-array-type (class (make-array Byte/TYPE 0)))

(defmulti coerce-blob
  "Coerces the blob to a vector of [size blob]. Where blob is
  a file or a byte array, but never a stream."
  ^File
  type)

(defmethod coerce-blob byte-array-type
  [^"[B" input]
  [(alength input) input])

(defmethod coerce-blob File
  [^File input]
  [(.length input) input])

;; this one will copy the input stream to a
;; temporary file so avoid if possible
(defmethod coerce-blob InputStream
  [^InputStream input]
  (let [temp-file (File/createTempFile "blob-storage" ".bin")]
    (io/copy input temp-file)
    (coerce-blob temp-file)))

(defmulti ^"[B" blob->bytes
  "Use to get a byte array from a returned blob. An optional size
   argument can be used as a hint of the blob's size to improve
   performance."
  (fn [blob & [size]]
    (type blob)))

(defmethod blob->bytes byte-array-type
  [^"[B" input & _]
  input)

(defmethod blob->bytes File
  [^File file & _]
  (let [result (byte-array (.length file))]
    (with-open [stream (io/input-stream file)]
      ;; ok with the underlying implementation of readBytes
      (.read stream result))
    result))

(defmethod blob->bytes InputStream
  [^InputStream input-stream & [size]]
  (if size
    ;; if the size is known then read directly to the byte array
    (let [result (byte-array size)]
      (.read input-stream result)
      result)
    ;; size is unknown, copy data chunk by chunk
    (with-open [os (java.io.ByteArrayOutputStream.)]
      (io/copy input-stream os :buffer-size 4096)
      (.toByteArray os))))

(defn blob->stream
  "Creates an InputStream from a blob. The blob can be a file or a byte array"
  [blob]
  (io/input-stream blob))
