MongoDB Ruby Driver [![Code Climate][codeclimate-img]][codeclimate-url] [![Gem Version][rubygems-img]][rubygems-url]
-----
The officially supported Ruby driver for [MongoDB](http://www.mongodb.org).


Documentation
-------------

High level documentation and usage examples are located
[here](http://docs.mongodb.org/ecosystem/drivers/ruby/).

API docs can be found [here](http://api.mongodb.org/ruby/).


Support & Feedback
------------------

For issues, questions or feedback related to the Ruby driver, please look into
our [support channels](http://www.mongodb.org/about/support). Please
do not email any of the Ruby developers directly with issues or
questions - you're more likely to get an answer quickly on the [mongodb-user list](http://groups.google.com/group/mongodb-user) on Google Groups.


Bugs & Feature Requests
-----------------------

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
---------------

If you’ve identified a potential security related issue in a driver or any other
MongoDB project, please report it by following the [instructions here](http://docs.mongodb.org/manual/tutorial/create-a-vulnerability-report).


Running Tests
-------------

The driver uses RSpec as its primary testing tool. Most tests require a
running MongoDB server. To test the driver against a single-node (standalone)
deployment, first launch a server:

    mkdir /tmp/mrb
    mongod --dbpath /tmp/mrb --bind_ip 127.0.0.1 --setParameter enableTestCommands=1

... then run the tests:

    bundle exec rake

It is possible to run tests in a specific file, as well as use other
test invocations supported by RSpec:

    bundle exec rspec path/to/spec.rb:42

Note that certain user accounts have to be created for individual tests to
succeed, and they are not created when the individual tests are run. The
user accounts are created if you run `rake` as mentioned above, and you can
also create them by running:

    bundle exec rake spec:prepare

For further information about supported cluster configurations and how to
configure the test suite, please see the README in the spec directory.


Release History
---------------

Full release notes and release history are available [here](https://github.com/mongodb/mongo-ruby-driver/releases).


License
-------

 Copyright (C) 2009-2019 MongoDB, Inc.

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
[codeclimate-img]: https://codeclimate.com/github/mongodb/mongo-ruby-driver.svg?branch=master
[codeclimate-url]: https://codeclimate.com/github/mongodb/mongo-ruby-driver?branch=master
