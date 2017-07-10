MongoDB Ruby Driver [![Build Status][travis-img]][travis-url] [![Code Climate][codeclimate-img]][codeclimate-url] [![Gem Version][rubygems-img]][rubygems-url]
-----
The officially supported Ruby driver for [MongoDB](http://www.mongodb.org).

Documentation
-----

Documentation is located [here](http://docs.mongodb.org/ecosystem/drivers/ruby/).

API docs can be found [here](http://api.mongodb.org/ruby/).

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

Running Tests
-----

The driver uses RSpec as it's primary testing tool. To run all tests simple run `rspec`.

To run a test at a specific location (where `42` is the line number), use:

    rspec path/to/spec.rb:42

Security Issues
-----

If you’ve identified a potential security related issue in a driver or any other
MongoDB project, please report it by following the [instructions here](http://docs.mongodb.org/manual/tutorial/create-a-vulnerability-report).

Release History
-----

Full release notes and release history are available [here](https://github.com/mongodb/mongo-ruby-driver/releases).

License
-----

 Copyright (C) 2009-2017 MongoDB, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

[rubygems-img]: https://badge.fury.io/rb/mongo.svg
[rubygems-url]: http://badge.fury.io/rb/mongo
[travis-img]: https://secure.travis-ci.org/mongodb/mongo-ruby-driver.svg?branch=master
[travis-url]: http://travis-ci.org/mongodb/mongo-ruby-driver?branch=master
[codeclimate-img]: https://codeclimate.com/github/mongodb/mongo-ruby-driver.svg?branch=master
[codeclimate-url]: https://codeclimate.com/github/mongodb/mongo-ruby-driver?branch=master
