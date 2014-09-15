MongoDB Ruby Driver [![Build Status][travis-img]][travis-url] [![Code Climate][codeclimate-img]][codeclimate-url] [![Coverage Status][coveralls-img]][coveralls-url] [![Gem Version][rubygems-img]][rubygems-url]
-----
The officially supported Ruby driver for [MongoDB](http://www.mongodb.org).

Installation
-----

**Gem Installation**<br>
The Ruby driver is released and distributed through RubyGems and it can be installed with the following command:

```bash
gem install mongo
```

For a significant performance boost, you'll want to install the C-extension:

```bash
gem install bson_ext
```

**Github Installation**<br>
For development and test environments (not recommended for production) you can also install the Ruby driver directly from source:

```bash
# clone the repository
git clone https://github.com/mongodb/mongo-ruby-driver.git
cd mongo-ruby-driver

# checkout a specific version by tag (optional)
git checkout 1.x.x

# install all development dependencies
gem install bundler
bundle install

# install the ruby driver
rake install
```

To be able to use the driver with Kerberos authentication enabled, install the
`mongo_kerberos` gem and add it instead of mongo to your application:

```bash
gem install mongo_kerberos
```

```ruby
require 'mongo_kerberos'
```

Usage
-----
Here is a quick example of basic usage for the Ruby driver:

```ruby
require 'mongo'
include Mongo

# connecting to the database
client = MongoClient.new # defaults to localhost:27017
db     = client['example-db']
coll   = db['example-collection']

# inserting documents
10.times { |i| coll.insert({ :count => i+1 }) }

# finding documents
puts "There are #{coll.count} total documents. Here they are:"
coll.find.each { |doc| puts doc.inspect }

# updating documents
coll.update({ :count => 5 }, { :count => 'foobar' })

# removing documents
coll.remove({ :count => 8 })
coll.remove
```

Wiki - Tutorials & Examples
-----
For many more usage examples and a full tutorial, please visit our [wiki](https://github.com/mongodb/mongo-ruby-driver/wiki).<br>

API Reference Documentation
-----
For API reference documentation, please visit [here](http://api.mongodb.org/ruby).

Compatibility
-----
The MongoDB Ruby driver requires Ruby 1.8.7 or greater and is regularly tested against the platforms and environments listed below.

Ruby Platforms | Operating Systems | Architectures
-------------- | ----------------- | -------------
MRI 1.8.7, 1.9.3, 2.0.0<br>JRuby 1.7.x | Windows<br>Linux<br>OS X | x86<br>x64<br>ARM

Support & Feedback
-----

**Support Channels**

For issues, questions or feedback related to the Ruby driver, please look into our [support channels](http://www.mongodb.org/about/support).
Please do not email any of the Ruby developers directly with issues or questions. You'll get a quicker answer on the [mongodb-user list](http://groups.google.com/group/mongodb-user) Google Group.

Bugs & Feature Requests
-----

Do you have a bug to report or a feature request to make?

1. Visit [our issue tracker](https://jira.mongodb.org) and login (or create an account if necessary).
2. Navigate to the [RUBY](https://jira.mongodb.org/browse/RUBY) project.
3. Click 'Create Issue' and fill out all the applicable form fields.

When reporting an issue, please keep in mind that all information in JIRA for all driver projects (ex. RUBY, CSHARP, JAVA) and the Core Server (ex. SERVER) project is **PUBLICLY** visible.

**HOW TO ASK FOR HELP**

Providing enough information so we can reproduce the issue immediately will reduce roundtrip communications and get you a useful response as quickly as possible.
That said, please provide the following information when logging an issue:

1. Environment
2. Ruby version, including patch-level
3. MongoDB version
4. A test case or code snippets
5. Stack traces and log data, keeping in mind that this info is public

**PLEASE DO NOT**

* Provide any sensitive data or server logs.
* Report potential security issues publicly (see 'Security Issues').

**EXAMPLE BUG REPORT**

Example taken from [RUBY-775](https://jira.mongodb.org/browse/RUBY-775)

```
There appears to be a recursive locking condition in the replica set connection pooling.

Environment: AWS Linux 3.10.37-47.135.amzn1.x86_64 / jruby-1.7.12 / JDK java-1.7.0-openjdk-1.7.0.55-2.4.7.1.40.amzn1.x86_64

Component: Connection Pooling / Replica set

Here is a stack trace:
https://gist.githubusercontent.com/cheald/5ed01172c5b2c9943c87/raw/63075158dac4c78c1775cac8bf84ba3b6537bc1e/gistfile1.txt

The original lock occurs [here](https://github.com/mongodb/mongo-ruby-driver/blob/1.x-stable/lib/mongo/connection/pool_manager.rb#L60)

and then the process of reconnecting ends up attempting to resynchronize the same lock [here](https://github.com/mongodb/mongo-ruby-driver/blob/1.x-stable/lib/mongo/connection/pool_manager.rb#L150)
```

Security Issues
-----

If you’ve identified a potential security related issue in a driver or any other MongoDB project, please report it by following the [instructions here](http://docs.mongodb.org/manual/tutorial/create-a-vulnerability-report).

Release History
-----

Full release notes and release history are available [here](https://github.com/mongodb/mongo-ruby-driver/releases).

License
-----

 Copyright (C) 2009-2013 MongoDB, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

[rubygems-img]: https://badge.fury.io/rb/mongo.png
[rubygems-url]: http://badge.fury.io/rb/mongo
[travis-img]: https://secure.travis-ci.org/mongodb/mongo-ruby-driver.png?branch=1.x-stable
[travis-url]: http://travis-ci.org/mongodb/mongo-ruby-driver?branch=1.x-stable
[codeclimate-img]: https://codeclimate.com/github/mongodb/mongo-ruby-driver.png?branch=1.x-stable
[codeclimate-url]: https://codeclimate.com/github/mongodb/mongo-ruby-driver?branch=1.x-stable
[coveralls-img]: https://coveralls.io/repos/mongodb/mongo-ruby-driver/badge.png?branch=1.x-stable
[coveralls-url]: https://coveralls.io/r/mongodb/mongo-ruby-driver?branch=1.x-stable
