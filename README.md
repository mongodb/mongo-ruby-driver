MongoDB Ruby Driver [![Build Status][travis-img]][travis-url] [![Code Climate][codeclimate-img]][codeclimate-url] [![Coverage Status][coveralls-img]][coveralls-url] [![Gem Version][rubygems-img]][rubygems-url]
-----
The officially supported Ruby driver for [MongoDB](http://www.mongodb.org).

> **Note: You are viewing the 2.x version of the MongoDB Ruby driver which is currently unreleased and under heavy development. To view the current stable version of driver, please use the [1.x-stable](https://github.com/mongodb/mongo-ruby-driver/tree/1.x-stable) branch.**


Installation
-----

**Gem Installation**<br>
The Ruby driver is released and distributed through RubyGems and it can be installed with the following command:
```bash
gem install mongo
```
**Github Installation**<br>
For development and test environments (not recommended for production) you can also install the Ruby driver directly from source:

```bash
# clone the repository
git clone https://github.com/mongodb/mongo-ruby-driver.git
cd mongo-ruby-driver

# checkout a specific version by tag (optional)
git checkout 2.x.x

# install all development dependencies
gem install bundler
bundle install

# install the ruby driver
rake install
```

Usage
-----
Here is a quick example of basic usage for the Ruby driver:
```ruby
require 'mongo'

# connecting to the database
client = Mongo::Client.new # defaults to localhost:27017
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

For many more usage examples and a full tutorial, please visit our [wiki](https://github.com/mongodb/mongo-ruby-driver/wiki).<br>
For API reference documentation, please visit [here](http://api.mongodb.org/ruby/current).

Compatibility
-----
The MongoDB Ruby driver requires Ruby 1.8.7 or greater and is regularly tested against the platforms and environments listed below.

Ruby Platforms | Operating Systems | Architectures
-------------- | ----------------- | -------------
MRI 1.8.7, 1.9.3, 2.0.0<br>JRuby 1.7.x<br>Rubinius 2.x | Windows<br>Linux<br>OS X | x86<br>x64<br>ARM

Support & Feedback
-----

For issues, questions or feedback related to the Ruby driver, please look into
our [support channels](http://www.mongodb.org/about/support). Please
do not email any of the Ruby developers directly with issues or
questions - you're more likely to get an answer quickly on the [mongodb-user list](http://groups.google.com/group/mongodb-user) on Google Groups.

Bugs & Feature Requests
-----

Do you have a bug to report or a feature request to make?

1. Visit [our issue tracker](https://jira.mongodb.org) and login (or create an account if necessary).
2. Navigate to the [RUBY](https://jira.mongodb.org/browse/RUBY) project.
3. Click 'Create Issue' and fill out all the applicable form fields.

When reporting an issue, please keep in mind that all information in JIRA for all driver projects (ex. RUBY, CSHARP, JAVA) and the Core Server (ex. SERVER) project is **PUBLICLY** visible.

**PLEASE DO**

* Provide as much information as possible about the issue.
* Provide detailed steps for reproducing the issue.
* Provide any applicable code snippets, stack traces and log data.
* Specify version information for the driver and MongoDB.

**PLEASE DO NOT**

* Provide any sensitive data or server logs.
* Report potential security issues publicly (see 'Security Issues').

Security Issues
-----

If you’ve identified a potential security related issue in a driver or any other
MongoDB project, please report it by following the [instructions here](http://docs.mongodb.org/manual/tutorial/create-a-vulnerability-report).

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
[travis-img]: https://secure.travis-ci.org/mongodb/mongo-ruby-driver.png?branch=master
[travis-url]: http://travis-ci.org/mongodb/mongo-ruby-driver?branch=master
[codeclimate-img]: https://codeclimate.com/github/mongodb/mongo-ruby-driver.png?branch=master
[codeclimate-url]: https://codeclimate.com/github/mongodb/mongo-ruby-driver?branch=master
[coveralls-img]: https://coveralls.io/repos/mongodb/mongo-ruby-driver/badge.png?branch=master
[coveralls-url]: https://coveralls.io/r/mongodb/mongo-ruby-driver?branch=master
