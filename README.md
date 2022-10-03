MongoDB Ruby Driver
[![Gem Version][rubygems-img]][rubygems-url]
[![Inline docs][inch-img]][inch-url]
================================================================

The officially supported Ruby driver for [MongoDB](https://www.mongodb.org/).

The Ruby driver supports Ruby 2.5-3.0 and JRuby 9.2.

## Documentation

High level documentation and usage examples are located
[here](https://www.mongodb.com/docs/ecosystem/drivers/ruby/).

API documentation for the most recent release can be found
[here](https://mongodb.com/docs/ruby-driver/current/api/).
To build API documentation for the master branch, check out the
repository locally and run `rake docs`.

## Support

Commercial support for the driver is available through the
[MongoDB Support Portal](https://support.mongodb.com/).

For questions, discussions or general technical support, please visit the
[MongoDB Community Forum](https://developer.mongodb.com/community/forums/tags/c/drivers-odms-connectors/7/ruby-driver).

Please see [Technical Support](https://mongodb.com/docs/manual/support/) page
in the documentation for other support resources.

## Bugs & Feature Requests

To report a bug in the driver or request a feature specific to the Ruby driver:

1. Visit [our issue tracker](https://jira.mongodb.org/) and login
   (or create an account if you do not have one already).
2. Navigate to the [RUBY project](https://jira.mongodb.org/browse/RUBY).
3. Click 'Create Issue' and fill out all of the applicable form fields.

When creating an issue, please keep in mind that all information in JIRA
for the RUBY project, as well as the core server (the SERVER project),
is publicly visible.

**PLEASE DO:**

- Provide as much information as possible about the issue.
- Provide detailed steps for reproducing the issue.
- Provide any applicable code snippets, stack traces and log data.
  Do not include any sensitive data or server logs.
- Specify version numbers of the driver and MongoDB server.

**PLEASE DO NOT:**

- Provide any sensitive data or server logs.
- Report potential security issues publicly (see 'Security Issues' below).

## Security Issues

If you have identified a potential security-related issue in the Ruby driver
(or any other MongoDB product), please report it by following the
[instructions here](https://www.mongodb.com/docs/manual/tutorial/create-a-vulnerability-report).

## Product Feature Requests

To request a feature which is not specific to the Ruby driver, or which
affects more than the driver alone (for example, a feature which requires
MongoDB server support), please submit your idea through the
[MongoDB Feedback Forum](https://feedback.mongodb.com/forums/924286-drivers).

## Maintenance and Bug Fix Policy

New driver functionality is generally added in a backwards-compatible manner
and results in new minor driver releases (2.x). Bug fixes are generally made on
master first and are backported to the current minor driver release. Exceptions
may be made on a case-by-case basis, for example security fixes may be
backported to older stable branches. Only the most recent minor driver release
is officially supported. Customers should use the most recent driver release in
their applications.

## Running Tests

Please refer to [spec/README.md](spec/README.md) for instructions on how
to run the driver's test suite.

## Release History

Full release notes and release history are available [on the GitHub releases
page](https://github.com/mongodb/mongo-ruby-driver/releases).

## License

Copyright (C) 2009-2020 MongoDB, Inc.

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
[inch-img]: http://inch-ci.org/github/mongodb/mongo-ruby-driver.svg?branch=master
[inch-url]: http://inch-ci.org/github/mongodb/mongo-ruby-driver
