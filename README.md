Go-Mongo
========

Go-Mongo is a [MongoDB](http://www..mongodb.org/) driver for the
[Go](http://golang.org) programming language.

Installation
------------

Go-Mongo requires a working Go development environment. The 
[Getting Started](http://golang.org/doc/install.html) 
document describes how to install the development environment. Once you have Go
up and running, you can install Go-Mongo with a single command:

    goinstall github.com/garyburd/go-mongo

The Go distribution is Go-Mongo's only dependency. 
  
Documentation
-------------
 
 * [Package Reference](http://gopkgdoc.appspot.com/pkg/github.com/garyburd/go-mongo)
 * [Examples](https://github.com/garyburd/go-mongo-examples)

Example
-------

    package main

    import (
        "github.com/garyburd/go-mongo"
        "log"
    )

    type ExampleDoc struct {
        Id    mongo.ObjectId "_id"
        Title string
        Body  string
    }

    func main() {

        // Connect to server.

        conn, err := mongo.Dial("localhost")
        if err != nil {
            log.Fatal(err)
        }
        defer conn.Close()

        c := mongo.Collection{conn, "example-db.example-collection", mongo.DefaultLastErrorCmd}

        // Insert a document.

        id := mongo.NewObjectId()

        err = c.Insert(&ExampleDoc{Id: id, Title: "Hello", Body: "Mongo is fun."})
        if err != nil {
            log.Fatal(err)
        }

        // Find the document.

        var doc ExampleDoc
        err = c.Find(map[string]interface{}{"_id": id}).One(&doc)
        if err != nil {
            log.Fatal(err)
        }

        log.Print(doc.Title, " ", doc.Body)
    }

License
-------

Go-Mongo is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

Discussion
----------
 
Discussion related to the use and development of Go-Mongo is held at the
[Go-Mongo User Group](http://groups.google.com/group/go-mongo-users).

You can also contact the authorthrough [Github](https://github.com/inbox/new/garyburd).

Development
-----------

Follow the development of Go-Mongo on [Github](http://github.com/garyburd/go-mongo).
