# blob-storage

A Clojure library designed to store blob files.

The rationale behind is to provide a standard API to store, read and delete blob files, independently of the storage used.

Intended to be a companion for [Datomic](http://www.datomic.com/), given the lack of support for blob files:

>When is Datomic not a good fit?

>Datomic is not a good fit if you need unlimited write scalability, or have data with a high update churn rate (e.g. counters).
And, at present, Datomic does not have support for BLOBs.
[faq](http://www.datomic.com/faq.html)


## Storages

Supported storages:

- [PostgreSQL](www.postgresql.org/)
    
Planned:

- FileSystem
- [Amazon S3](https://aws.amazon.com/s3/)
- [Cassandra](http://cassandra.apache.org/)


## Usage

#### Blobs types

We consider a blob one of the following types:

* [java.io.ByteArrayOutputStream](http://docs.oracle.com/javase/7/docs/api/java/io/ByteArrayOutputStream.html)
* [java.sql.Blob](http://docs.oracle.com/javase/7/docs/api/java/sql/Blob.html)
* [java.io.InputStream](http://docs.oracle.com/javase/7/docs/api/java/io/InputStream.html)

#### Public Protocol

```clojure
(ns blob-storage.api)

(defprotocol BlobStorage

  (store! [service blob opts]
    "Adds a new blob to the database")
    
  (update! [service id blob]
    "Updates a blob from the database")

  (del! [service id]
    "Delete a blob from the database")

  (blob [service id]
    "Retrieves a blob given its id")

  (drop-schema! [service]
    "Drops the schema")

  (init-schema! [service]
    "Initializes schema"))
```


#### API

###### Defining a PosgreSQL service for your app:

```clojure
(def db-uri (or (System/getenv "DATABASE_URL")
  "postgresql://postgres:postgres@localhost:5432/blobs"))

(require '[blob-storage.postgres :as p])
(require '[blob-storage.api :as api])

(def service (p/make db-uri))
(api/init-schema! service)
```

###### Storing Blobs

```clojure
(require '[blob-storage.api :as api])

(api/store! service
            (byte-array [(byte 0) (byte 1) (byte 2)]))
;; "d69b8dc8-53f7-4d67-b71a-8cbd4d125e81"
```

###### Updating Blobs

```clojure
(require '[blob-storage.api :as api])

(api/update! service
            "d69b8dc8-53f7-4d67-b71a-8cbd4d125e81"
            (byte-array [(byte 0)]))
;; "d69b8dc8-53f7-4d67-b71a-8cbd4d125e81"
```

###### Reading Blobs

```clojure
(require '[blob-storage.api :as api])

(api/blob service "d69b8dc8-53f7-4d67-b71a-8cbd4d125e81")
;; {:id d69b8dc8-53f7-4d67-b71a-8cbd4d125e81,
;;  :blob #object[[B 0x4c2bf85e [B@4c2bf85e],
;;  :created_at #inst "2015-09-14T14:01:15.049787000-00:00",
;;  :updated_at #inst "2015-09-14T15:04:20.061402000-00:00"}

```

###### Deleting Blobs

```clojure
(require '[blob-storage.api :as api])

(api/del! service "d69b8dc8-53f7-4d67-b71a-8cbd4d125e81")
```


#### Storing files

```clojure
(require '[blob-storage.api :as api])

(api/store! service (clojure.java.io/input-stream "/some/file.txt"))  
;; "d69b8dc8-53f7-4d67-b71a-8cbd4d125e81"
    
```

## Testing

###### bash
```bash
$ export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/blobs_test" && lein test
```

###### fish
```bash
$ set -x DATABASE_URL "postgresql://postgres:postgres@localhost:5432/blobs_test"; lein test
```


License
----

Copyright © 2015 Intception

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
