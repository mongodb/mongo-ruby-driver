# MongoDB Ruby Driver Tutorial

This tutorial gives many common examples of using MongoDB with the Ruby driver. If you're looking for information on data modeling, see [MongoDB Data Modeling and Rails](http://www.mongodb.org/display/DOCS/MongoDB+Data+Modeling+and+Rails). Links to the various object mappers are listed on our [object mappers page](http://www.mongodb.org/display/DOCS/Object+Mappers+for+Ruby+and+MongoDB).

Interested in GridFS? See [GridFS in Ruby](file.GridFS.html).

As always, the [latest source for the Ruby driver](http://github.com/mongodb/mongo-ruby-driver) can be found on [github](http://github.com/mongodb/mongo-ruby-driver/).

## Installation

The mongo-ruby-driver gem is served through Rubygems.org. To install, make sure you have the latest version of rubygems.

    gem update --system

Next, install the mongo rubygem:

    gem install mongo

The required `bson` gem will be installed automatically.

For optimum performance, install the bson\_ext gem:

    gem install bson_ext

After installing, you may want to look at the [examples](http://github.com/mongodb/mongo-ruby-driver/tree/master/examples) directory included in the source distribution. These examples walk through some of the basics of using the Ruby driver.

## Getting started

Note that the output in the following has been updated to Ruby 1.9, so if you are using Ruby 1.8, you will see some minor differences.  To follow this tutorial interactively, at the command line, run the Interactive Ruby Shell.

    irb

As you execute commands, irb will output the result using the `inspect` method.  If you are editing and running a script for this tutorial, you can view output using the `puts` or `p` methods.

### Using the gem

Use the `mongo` gem via the `require` kernel method.

    require 'rubygems'  # not necessary for Ruby 1.9
    require 'mongo'

### Making a Connection

An `Mongo::Connection` instance represents a connection to MongoDB.  You can optionally specify the MongoDB server address and port when connecting. The following example shows three ways to connect to the local machine:

    connection = Mongo::Connection.new # (optional host/port args)
    connection = Mongo::Connection.new("localhost")
    connection = Mongo::Connection.new("localhost", 27017)

### Listing All Databases

    connection.database_names
    connection.database_info.each { |info| puts info.inspect }

The `database_info` method returns a hash mapping database names to the size of the database in bytes.

## Using a Database

You use a Connection instance to obtain an Mongo::DB instance, which represents a named database. The database doesn't have to exist - if it doesn't, MongoDB will create it for you. The following examples use the database "mydb":

    db = connection.db("mydb")
    db = Mongo::Connection.new.db("mydb")

At this point, the `db` object will be a connection to a MongoDB server for the specified database. Each DB instance uses a separate socket connection to the server.

If you're trying to connect to a replica set, see [Replica Sets in Ruby](http://www.mongodb.org/display/DOCS/Replica+Sets+in+Ruby).

### Authentication

MongoDB can be run in a secure mode where access to databases is controlled through name and password authentication.  When run in this mode, any client application must provide a name and password before doing any operations.  In the Ruby driver, you simply do the following with the connected mongo object:

    auth = db.authenticate(my_user_name, my_password)

If the name and password are valid for the database, `auth` will be `true`.  Otherwise, it will be `false`.  You should look at the MongoDB log for further information if available.

## Using a Collection

You can get a collection to use using the `collection` method:

    coll = db.collection("testCollection")

This is aliased to the \[\] method:

    coll = db["testCollection"]

Once you have this collection object, you can now do create, read, update, and delete (CRUD) functions on persistent storage.

### Creating Documents with `insert`

Once you have the collection object, you can create or `insert` documents into the collection.  For example, lets make a little document that in JSON would be represented as

      {
         "name" : "MongoDB",
         "type" : "database",
         "count" : 1,
         "info" : {
                     x : 203,
                     y : 102
                   }
      }

Notice that the above has an "inner" document embedded within it.  To do this, we can use a Hash or the driver's OrderedHash (which preserves key order) to create the document (including the inner document), and then just simply insert it into the collection using the `insert` method.

    doc = {"name" => "MongoDB", "type" => "database", "count" => 1, "info" => {"x" => 203, "y" => '102'}}
    id = coll.insert(doc)

We have saved the `id` for future use below.  Now the collection has been created and you can list it.

#### Getting a List Of Collections

Each database has zero or more collections.  You can retrieve a list of them from the db (and print out any that are there):

	db.collection_names

You should see

	\["testCollection", "system.indexes"\]

#### Adding Multiple Documents

To demonstrate some more interesting queries, let's add multiple simple documents to the collection.  These documents will have the following form:

	{
	    "i" : value
	}

Here's how to insert them:

	100.times { |i| coll.insert("i" => i) }

Notice that we can insert documents of different "shapes" into the same collection. These records are in the same collection as the complex record we inserted above.  This aspect is what we mean when we say that MongoDB is "schema-free".

### Reading Documents with `find_one` and `find`

#### Reading the First Document in a Collection using `find_one`

To retrieve the document that we inserted, we can do a simple `find_one` method to get the first document in the collection.  This method returns a single document directly.

	coll.find_one

and you should something like:

	{"_id"=>BSON::ObjectId('4f7b1ea6e4d30b35c9000001'), "name"=>"MongoDB", "type"=>"database", "count"=>1, "info"=>{"x"=>203, "y"=>"102"}}

Note the `_id` element has been added automatically by MongoDB to your document.

#### Reading All of the Documents with a Cursor using `find`

To get all the documents from the collection, we use the `find` method. `find` returns a `Cursor` object, which allows us to iterate over the set of documents that matches our query.  The Ruby driver's Cursor implemented Enumerable, which allows us to use `Enumerable#each`, `Enumerable#map}, etc. For instance:

	coll.find.each { |row| puts row.inspect }

and that should print all 101 documents in the collection.  You can take advantage of `Enumerable#to_a`.

    puts coll.find.to_a

Important note - using `to_a` pulls all of the full result set into memory and is inefficient if you can process by each individual document.  To process with more memory efficiency, use the `each` method with a code block for the cursor.

#### Specific Queries

We can create a _query_ hash to pass to the `find` method to get a subset of the documents in our collection.  To check that our update worked, find the document by id:

    coll.find("_id" => id).to_a

If we wanted to find the document for which the value of the "i" field is 71, we would do the following:

    coll.find("i" => 71).to_a

and it should just print just one document:

    {"_id"=>BSON::ObjectId('4f7b20b4e4d30b35c9000049'), "i"=>71}

#### Sorting Documents in a Collection

To sort documents, simply use the `sort` method. The method can either take a key or an array of [key, direction] pairs to sort by.

Direction defaults to ascending order but can be specified as Mongo::ASCENDING, :ascending, or :asc whereas descending order can be specified with Mongo::DESCENDING, :descending, or :desc.

    # Sort in ascending order by :i
    coll.find.sort(:i)

    # Sort in descending order by :i
    coll.find.sort([:i, :desc])

#### Counting Documents in a Collection

Now that we've inserted 101 documents (the 100 we did in the loop, plus the first one), we can check to see if we have them all using the `count` method.

    coll.count

and it should print `101`.

#### Getting a Set of Documents With a Query

We can use the query to get a set of documents from our collection.  For example, if we wanted to get all documents where "i" > 50, we could write:

    puts coll.find("i" => {"$gt" => 50}).to_a

which should print the documents where i > 50.  We could also get a range, say   20 < i <= 30:

    puts coll.find("i" => {"$gt" => 20, "$lte" => 30}).to_a

#### Selecting a Subset of Fields for a Query

Use the `:fields` option to specify fields to return.

    puts coll.find("_id" => id, :fields => ["name", "type"]).to_a

#### Querying with Regular Expressions

Regular expressions can be used to query MongoDB. To find all names that begin with 'a':

    puts coll.find({"name" => /^M/}).to_a

You can also construct a regular expression dynamically. To match a given search string:

    params = {'search' => 'DB'}
    search_string = params['search']

    # Constructor syntax
    puts coll.find({"name" => Regexp.new(search_string)}).to_a

    # Literal syntax
    puts coll.find({"name" => /#{search_string}/}).to_a

Although MongoDB isn't vulnerable to anything like SQL-injection, it may be worth checking the search string for anything malicious.

### Updating Documents with `update`

We can update the previous document using the `update` method. There are a couple ways to update a document. We can rewrite it:

    doc["name"] = "MongoDB Ruby"
    coll.update({"_id" => id}, doc)

Or we can use an atomic operator to change a single value:

    coll.update({"_id" => id}, {"$set" => {"name" => "MongoDB Ruby"}})

Verify the update.

    puts coll.find("_id" => id).to_a

Read [more about updating documents|Updating].

### Deleting Documents with `remove`

Use the `remove` method to delete documents.

    coll.count
    coll.remove("i" => 71)
    coll.count
    puts coll.find("i" => 71).to_a

The above shows that the count has been reduced and that the document can no longer be found.

Without arguments, the `remove` method deletes all documents.

    coll.remove
    coll.count

Please program carefully.

## Indexing

### Creating An Index

MongoDB supports indexes, and they are very easy to add on a collection.  To create an index, you specify an index name and an array of field names to be indexed, or a single field name. The following creates an ascending index on the "i" field:

    # create_index assumes ascending order; see method docs
    # for details
    coll.create_index("i")
To specify complex indexes or a descending index you need to use a slightly more complex syntax - the index specifier must be an Array of [field name, direction] pairs. Directions should be specified as Mongo::ASCENDING or Mongo::DESCENDING:

    # Explicit "ascending"
    coll.create_index([["i", Mongo::ASCENDING]])

Use the `explain` method on the cursor to show how MongoDB will run the query.

    coll.find("_id" => id).explain
    coll.find("i" => 71).explain
    coll.find("type" => "database").explain

The above shows that the query by `_id` and `i` will use faster indexed BtreeCursor, while the query by `type` will use a slower BasicCursor.

### Getting a List of Indexes on a Collection

You can get a list of the indexes on a collection.

    coll.index_information

### Creating and Querying on a Geospatial Index

First, create the index on a field containing long-lat values:

    people.create_index([["loc", Mongo::GEO2D]])

Then get a list of the twenty locations nearest to the point 50, 50:

    people.find({"loc" => {"$near" => [50, 50]}}, {:limit => 20}).each do |p|
      puts p.inspect
    end

## Dropping

### Drop an Index

To drop a secondary index, use the `drop_index` method on the collection.

    coll.drop_index("i_1")
    coll.index_information

The dropped index is no longer listed.

### Drop All Indexes

To drop all secondary indexes, use the `drop_indexes` method on the collection.

    coll.drop_indexes
    coll.index_information

Only the primary index "_id_" is listed.

### Drop a Collection

To drop a collection, use the `drop` method on the collection.

    coll.drop
    db.collection_names

The dropped collection is no longer listed.  The `drop_collection` method can be used on the database as an alternative.

    db.drop_collection("testCollection")

### Drop a Database

To drop a database, use the `drop_database` method on the connection.

    connection.drop_database("mydb")
    connection.database_names

The dropped database is no longer listed.

## Database Administration

A database can have one of three profiling levels: off (:off), slow queries only (:slow_only), or all (:all). To see the database level:

    puts db.profiling_level   # => off (the symbol :off printed as a string)
    db.profiling_level = :slow_only

Validating a collection will return an interesting hash if all is well or raise an exception if there is a problem.
    p db.validate_collection('coll_name')

## See Also

* [MongoDB Koans](http://github.com/chicagoruby/MongoDB_Koans) A path to MongoDB enlightenment via the Ruby driver.
* [MongoDB Manual](http://www.mongodb.org/display/DOCS/Developer+Zone)
