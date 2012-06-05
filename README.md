# Documentation

This API documentation is available online at [http://api.mongodb.org/ruby](http://api.mongodb.org/ruby)
for all releases of the MongoDB Ruby driver.  Please reference the exact version of the documentation
that matches the release of the Ruby driver that you are using.  Note that the
[Ruby Language Center for MongoDB](http://www.mongodb.org/display/DOCS/Ruby+Language+Center)
has a link to API Documentation for the current release.

If you have the source, you can generate the matching documentation  by typing

    $ rake ydoc

Then open the file ydoc/index.html in your browser.

# Introduction

This is the 10gen-supported Ruby driver for [MongoDB](http://www.mongodb.org).

For the reference manual, use the links in the upper-left and upper-right corners for quick navigation to the following.

* [Alphabetic Index](_index.html)
* [Class List](class_list.html)
* [Method List](method_list.html)
* [File List](file_list.html)

This documentation has other articles of interest, including:

1. [A tutorial](docs/TUTORIAL.md).
2. [Replica Sets in Ruby](docs/REPLICA_SETS.md).
3. [Write Concern in Ruby](docs/WRITE_CONCERN.md).
4. [Tailable Cursors in Ruby](docs/TAILABLE_CURSORS.md).
5. [Read Preference in Ruby](docs/READ_PREFERENCE.md).
6. [GridFS in Ruby](docs/GridFS.md).
7. [Frequently Asked Questions](docs/FAQ.md).
8. [History](docs/HISTORY.md).
9. [Release plan](docs/RELEASES.md).
10. [Credits](docs/CREDITS.md).

Here's a quick code sample. Again, see the [MongoDB Ruby Tutorial](docs/TUTORIAL.md)
for much more:

    require 'rubygems'
    require 'mongo'

    @conn = Mongo::Connection.new
    @db   = @conn['sample-db']
    @coll = @db['test']

    @coll.remove
    3.times do |i|
      @coll.insert({'a' => i+1})
    end

    puts "There are #{@coll.count} records. Here they are:"
    @coll.find.each { |doc| puts doc.inspect }

# Installation

### Ruby Versions

The driver works and is consistently tested on Ruby 1.8.7 and 1.9.3 as well as JRuby 1.6.6.

Note that if you're on 1.8.7, be sure that you're using a patchlevel >= 249. There
are some IO bugs in earlier versions.

### Gems

The driver's gems are hosted at [Rubygems.org](http://rubygems.org). Make sure you're
using the latest version of rubygems:

    $ gem update --system

Then you can install the mongo gem as follows:

    $ gem install mongo

The driver also requires the bson gem:

    $ gem install bson

And for a significant performance boost, you'll want to install the C extensions:

    $ gem install bson_ext

Note that bson_ext isn't used with JRuby. Instead, some native Java extensions are bundled with the bson gem.
If you ever need to modify these extensions, you can recompile with the following rake task:

    $ rake build:java

### From the GitHub source

The source code is available at http://github.com/mongodb/mongo-ruby-driver.
You can either clone the git repository or download a tarball or zip file.
Once you have the source, you can use it from wherever you downloaded it or
you can install it as a gem from the source by typing

    $ rake gem:install

To install the C extensions from source, type this instead:

    $ rake gem:install_extensions

That's all there is to it!

# Examples

For extensive examples, see the [MongoDB Ruby Tutorial](http://api.mongodb.org/ruby/current/file.TUTORIAL.html).

Bundled with the driver are many examples, located in the "docs/examples" subdirectory. Samples include using
the driver and using the GridFS class GridStore. MongoDB must be running for
these examples to work, of course.

Here's how to start MongoDB and run the "simple.rb" example:

      $ cd path/to/mongo
      $ ./mongod run
      ... then in another window ...
      $ cd path/to/mongo-ruby-driver
      $ ruby docs/examples/simple.rb

See also the test code, especially test/test_db_api.rb.

# GridFS

The Ruby driver include two abstractions for storing large files: Grid and GridFileSystem.
The Grid class is a Ruby implementation of MongoDB's GridFS file storage
specification. GridFileSystem is essentially the same, but provides a more filesystem-like API
and assumes that filenames are unique.

An instance of both classes represents an individual file store. See the API reference
for details, and see examples/gridfs.rb for code that uses many of the Grid
features (metadata, content type, seek, tell, etc).

Examples:

      # Write a file on disk to the Grid
      file = File.open('image.jpg')
      grid = Mongo::Grid.new(db)
      id   = grid.put(file)

      # Retrieve the file
      file = grid.get(id)
      file.read

      # Get all the file's metata
      file.filename
      file.content_type
      file.metadata

# Notes

## Thread Safety

The driver is thread-safe.

## Connection Pooling

The driver implements connection pooling. By default, only one
socket connection will be opened to MongoDB. However, if you're running a
multi-threaded application, you can specify a maximum pool size and a maximum
timeout for waiting for old connections to be released to the pool.

To set up a pooled connection to a single MongoDB instance:

    @conn = Connection.new("localhost", 27017, :pool_size => 5, :timeout => 5)

Though the pooling architecture will undoubtedly evolve, it currently owes much credit
to the connection pooling implementations in ActiveRecord and PyMongo.

## Forking

Certain Ruby application servers work by forking, and it has long been necessary to
re-establish the child process's connection to the database after fork. But with the release
of v1.3.0, the Ruby driver detects forking and reconnects automatically.

## Environment variable `MONGODB_URI`

`Mongo::Connection.from_uri`, `Mongo::Connection.new` and `Mongo::ReplSetConnection.new` will use <code>ENV["MONGODB_URI"]</code> if no other args are provided.

The URI must fit this specification:

    mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]

If the type of connection (direct or replica set) should be determined entirely from <code>ENV["MONGODB_URI"]</code>, you may want to use `Mongo::Connection.from_uri` because it will return either `Mongo::Connection` or a `Mongo::ReplSetConnection` depending on how many hosts are specified. Trying to use `Mongo::Connection.new` with multiple hosts in <code>ENV["MONGODB_URI"]</code> will raise an exception.

## String Encoding

The BSON ("Binary JSON") format used to communicate with Mongo requires that
strings be UTF-8 (http://en.wikipedia.org/wiki/UTF-8).

Ruby 1.9 has built-in character encoding support. All strings sent to Mongo
and received from Mongo are converted to UTF-8 when necessary, and strings
read from Mongo will have their character encodings set to UTF-8.

When used with Ruby 1.8, the bytes in each string are written to and read from
Mongo as is. If the string is ASCII, all is well, because ASCII is a subset of
UTF-8. If the string is not ASCII, it may not be a well-formed UTF-8
string.

## Primary Keys

The `_id` field is a primary key. It is treated specially by the database, and
its use makes many operations more efficient. The value of an _id may be of
any type. The database itself inserts an _id value if none is specified when
a record is inserted.

### Primary Key Factories

A primary key factory is a class you supply to a DB object that knows how to
generate _id values. If you want to control _id values or even their types,
using a PK factory lets you do so.

You can tell the Ruby Mongo driver how to create primary keys by passing in
the :pk option to the Connection#db method.

    db = Mongo::Connection.new.db('dbname', :pk => MyPKFactory.new)

A primary key factory object must respond to :create_pk, which should
take a hash and return a hash which merges the original hash with any
primary key fields the factory wishes to inject.

NOTE: if the object already has a primary key, the factory should not
inject a new key; this means that the object may already exist in the
database.  The idea here is that whenever a record is inserted, the
:pk object's +create_pk+ method will be called and the new hash
returned will be inserted.

Here is a sample primary key factory, taken from the tests:

    class TestPKFactory
      def create_pk(row)
        row['_id'] ||= BSON::ObjectId.new
        row
      end
    end

Here's a slightly more sophisticated one that handles both symbol and string
keys. This is the PKFactory that comes with the MongoRecord code (an
ActiveRecord-like framework for non-Rails apps) and the AR Mongo adapter code
(for Rails):

    class PKFactory
      def create_pk(row)
        return row if row[:_id]
        row.delete(:_id)      # in case it exists but the value is nil
        row['_id'] ||= BSON::ObjectId.new
        row
      end
    end

A database's PK factory object may be set either when a DB object is created
or immediately after you obtain it, but only once. The only reason it is
changeable at all is so that libraries such as MongoRecord that use this
driver can set the PK factory after obtaining the database but before using it
for the first time.

## The DB Class

### Strict mode

Each database has an optional strict mode. If strict mode is on, then asking
for a collection that does not exist will raise an error, as will asking to
create a collection that already exists. Note that both these operations are
completely harmless; strict mode is a programmer convenience only.

To turn on strict mode, either pass in :strict => true when obtaining a DB
object or call the `:strict=` method:

    db = Connection.new.db('dbname', :strict => true)
    # I'm feeling lax
    db.strict = false
    # No, I'm not!
    db.strict = true

The method DB#strict? returns the current value of that flag.

## Cursors

Notes:

* Cursors are enumerable (and have a #to_a method).

* The query doesn't get run until you actually attempt to retrieve data from a
  cursor.

* Cursors will timeout on the server after 10 minutes. If you need to keep a cursor
  open for more than 10 minutes, specify `:timeout => false` when you create the cursor.

## Socket timeouts

The Ruby driver support timeouts on socket read operations. To enable them, set the
`:op_timeout` option when you create a `Mongo::Connection` object.

If implementing higher-level timeouts, using tools like `Rack::Timeout`, it's very important
to call `Mongo::Connection#close` to prevent the subsequent operation from receiving the previous
request.

### Test-Unit, Shoulda, and Mocha

Running the test suite requires test-unit, shoulda, and mocha.  You can install them as follows:

    $ gem install test-unit
    $ gem install shoulda
    $ gem install mocha

The tests assume that the Mongo database is running on the default port. You
can override the default host (localhost) and port (Connection::DEFAULT_PORT) by
using the environment variables MONGO_RUBY_DRIVER_HOST and
MONGO_RUBY_DRIVER_PORT.

# Testing

If you have the source code, you can run the tests.  Skip this test with the C extension if you're running JRuby.

    $ rake test:c

If you want to test the basic Ruby encoder, without the C extension, or if you're running JRuby:

    $ rake test:ruby

These will run both unit and functional tests. To run these tests alone:

    $ rake test:unit
    $ rake test:functional

To run any individual rake tasks with the C extension enabled, just pass C_EXT=true to the task (don't do this with JRuby):

    $ rake test:unit C_EXT=true

If you want to test replica set, you can run the following task:

    $ rake test:rs

# Release Notes

See HISTORY.

# Credits

See CREDITS.

# License

 Copyright 2008-2010 10gen Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
