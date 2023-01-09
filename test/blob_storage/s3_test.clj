(ns blob-storage.s3-test
  (:require [blob-storage.api :as b]
            [blob-storage.aws-s3 :as s3]
            [blob-storage.test-common :refer :all]
            [clojure.test :refer :all]
            [cognitect.aws.client.api :as aws]
            [environ.core :refer [env]]))

(def bucket-name "prisma-blob-storage-test")
(def access-key-id (env :aws-access-key-id))
(def access-key-secret (env :aws-access-key-secret))
(def region (or (env :aws-region) "us-east-1"))

(def service (s3/make {:region region
                       :access-key-id access-key-id
                       :access-key-secret access-key-secret
                       :bucket-name bucket-name}))

(defn init-schema-fixture [f]
  (try
    (->> (aws/invoke (:s3-client service) {:op :ListObjects
                                           :request {:Bucket bucket-name}})
         :Contents
         (map :Key)
         (map #(b/del! service %)))
    (b/drop-schema! service)
    (catch Exception e))
  (b/init-schema! service)
  (f))

(use-fixtures :each init-schema-fixture)

(deftest store-blob
  (test-store-blob service))

(deftest store-blob-with-id
  (test-store-blob-with-id service))

(deftest store-blob-with-tag
  (test-store-blob-with-tag service))

(deftest store-blob-with-id-and-tag
  (test-store-blob-with-id-and-tag service))

(deftest update-blob
  (test-update-blob service))

(deftest delete-blob
  (test-delete-blob service))
