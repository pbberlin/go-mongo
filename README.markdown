Go-Mongo
========

Go-Mongo is a [MongoDB](http://www..mongodb.org/) driver for the
[Go](http://golang.org) programming language.

Go-Mongo is designed to be used with mongos in production environments. If you
do not plan to use mongos, then [mgo](https://launchpad.net/mgo) is probably
the better driver to use.

Features:

* Marshalling and unmarshalling of documents to Go types using a built-in BSON encoder.
* Streaming result reader. The driver reduces latency and memory use by returning documents to the application before the complete result batch is received.
* Helpers for common database commands.
* Connection pooling.
* Simple and clean design. 

Installation
------------

Use the [go tool](http://weekly.golang.org/cmd/go/) to install Go-Mongo:

    go get  github.com/garyburd/go-mongo/mongo

Documentation
-------------
 
 * [Package Reference](http://gopkgdoc.appspot.com/pkg/github.com/garyburd/go-mongo/mongo)
 * [Example](https://github.com/garyburd/go-mongo/tree/master/examples/little-book)

License
-------

Go-Mongo is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

Discussion
----------
 
Discussion related to the use and development of Go-Mongo is held at the
[Go-Mongo User Group](http://groups.google.com/group/go-mongo-users).

You can also contact the author through [Github](https://github.com/inbox/new/garyburd).
