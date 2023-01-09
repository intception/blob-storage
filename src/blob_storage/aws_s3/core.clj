(ns blob-storage.aws-s3.core
  (:require [cognitect.aws.client.api :as aws]))

(defn- handle-error
  ([result]
   (handle-error result identity))
  ([result return-fn]
   (if-let [e (:cognitect.aws.client.impl/throwable result)]
     (throw e)
     (if-let [error (:Error result)]
       (throw (ex-info (:Message error) error))
       (return-fn result)))))

(defn create-bucket!
  [s3-client bucket-name]
  (let [bucket (aws/invoke s3-client {:op :CreateBucket
                                      :request {:Bucket bucket-name}})]
    ;; {:Location "<bucket-name>"}
    (handle-error bucket)))

(defn delete-bucket!
  [s3-client bucket-name]
  (let [bucket (aws/invoke s3-client {:op :DeleteBucket
                                      :request {:Bucket bucket-name}})]
    ;; {}
    (handle-error bucket)))

(defn put-object!
  [s3-client
   bucket-name
   key
   metadata
   [blob-size input-stream :as blob]]
  (let [object (aws/invoke s3-client {:op :PutObject
                                      :request (merge (when (not-empty metadata)
                                                        {:Metadata metadata})
                                                      {:Bucket bucket-name
                                                       :Key key
                                                       :ContentLength blob-size
                                                       :Body input-stream})})]
    ;; {:ETag "\"a63c90cc3684ad8b0a2176a6a8fe9005\""}
    (handle-error object :ETag)))

(declare get-object)

(defn update-object!
  [s3-client
   bucket-name
   key
   [blob-size input-stream :as blob]]
  (let [object (aws/invoke s3-client {:op :GetObject
                                      :request {:Bucket bucket-name
                                                :Key key}})]
    (put-object! s3-client bucket-name key (:Metadata object) blob)))

(defn delete-object!
  [s3-client bucket-name key]
  (let [result (aws/invoke s3-client {:op :DeleteObject
                                      :request {:Bucket bucket-name
                                                :Key key}})]
    (handle-error result)))

(defn get-object
  [s3-client bucket-name key]
  (let [object (aws/invoke s3-client {:op :GetObject
                                      :request {:Bucket bucket-name
                                                :Key key}})]
    (when (:ETag object)
      (merge (:Metadata object)
             {:id key
              :size (:ContentLength object)
              ;; creation date is not available, we could use metadata but we may risk
              ;; having some serialization/deserialization issues
              :created_at (:LastModified object)
              :updated_at (:LastModified object)
              ;; Body is an input stream.
              ;; TODO: check if it blocks the connection (we may need to copy it)
              :blob (:Body object)}))))
