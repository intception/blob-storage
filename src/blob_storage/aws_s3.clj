(ns blob-storage.aws-s3
  (:gen-class)
  (:require [blob-storage.api :refer [BlobStorage]]
            [blob-storage.aws-s3.core :as core]
            [blob-storage.coerce :refer [coerce-blob]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as aws-creds]))

(defrecord S3 [s3-client bucket-name]
  BlobStorage

  (init-schema! [_]
    (core/create-bucket! s3-client bucket-name))

  (drop-schema! [_]
    (core/delete-bucket! s3-client bucket-name))

  (store! [_ blob]
    (let [id (str (java.util.UUID/randomUUID))]
      (when (core/put-object! s3-client
                              bucket-name
                              id
                              {}
                              (coerce-blob blob))
        id)))

  (store! [_ blob {:keys [id tag] :as data}]
    (let [id (or id (str (java.util.UUID/randomUUID)))]
      (when (core/put-object! s3-client
                              bucket-name
                              id
                              (when tag
                                {:tag tag})
                              (coerce-blob blob))
        id)))

  (update! [_ id blob]
    (core/update-object! s3-client
                         bucket-name
                         id
                         (coerce-blob blob)))

  (del! [_ id]
    (core/delete-object! s3-client bucket-name id))

  (blob [service id]
    (core/get-object s3-client bucket-name id))

  (blob-metadata [_ id]
    ;; TODO: see if metadata is available in the response
    (when-let [blob (core/get-object s3-client bucket-name id)]
      (dissoc blob :blob))))

(defn make
  [{:keys [region
           access-key-id
           access-key-secret
           bucket-name]
    :or {bucket-name "prisma_blobs"}
    :as config}]
  (let [s3-client (aws/client {:api                  :s3
                               :region               (or region "us-east-1") ;; TODO: remove the OR
                               :credentials-provider (aws-creds/basic-credentials-provider
                                                       {:access-key-id     access-key-id
                                                        :secret-access-key access-key-secret})})]
    (S3. s3-client bucket-name)))
