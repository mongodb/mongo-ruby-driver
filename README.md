MongoDB Ruby Driver
[![Gem Version][rubygems-img]][rubygems-url]
[![Inline docs][inch-img]][inch-url]
================================================================

The officially supported Ruby driver for [MongoDB](https://www.mongodb.org/).

The Ruby driver supports Ruby 2.7-3.3 and JRuby 9.3-9.4.

## Installation

Install via RubyGems, either via the command-line for ad-hoc uses:

  $ gem install mongo

Or via a Gemfile for more general use:

  gem 'mongo'

### Release Integrity

Each release of the MongoDB Ruby driver after version 2.20.0 has been automatically built and signed using the team's GPG key.

To verify the driver's gem file:

1. [Download the GPG key](https://pgp.mongodb.com/ruby-driver.asc).
2. Import the key into your GPG keyring with `gpg --import ruby-driver.asc`.
3. Download the gem file (if you don't already have it). You can download it from RubyGems with `gem fetch mongo`, or you can download it from the [releases page](https://github.com/mongodb/mongo-ruby-driver/releases) on GitHub.
4. Download the corresponding detached signature file from the [same release](https://github.com/mongodb/mongo-ruby-driver/releases). Look at the bottom of the release that corresponds to the gem file, under the 'Assets' list, for a `.sig` file with the same version number as the gem you wish to install.
5. Verify the gem with `gpg --verify mongo-X.Y.Z.gem.sig mongo-X.Y.Z.gem` (replacing `X.Y.Z` with the actual version number).

You are looking for text like "Good signature from "MongoDB Ruby Driver Release Signing Key <packaging@mongodb.com>" in the output. If you see that, the signature was found to correspond to the given gem file.

(Note that other output, like "This key is not certified with a trusted signature!", is related to *web of trust* and depends on how strongly you, personally, trust the `ruby-driver.asc` key that you downloaded from us. To learn more, see https://www.gnupg.org/gph/en/manual/x334.html)

### Why not use RubyGems' gem-signing functionality?

RubyGems' own gem signing is problematic, most significantly because there is no established chain of trust related to the keys used to sign gems. RubyGems' own documentation admits that "this method of signing gems is not widely used" (see https://guides.rubygems.org/security/). Discussions about this in the RubyGems community have been off-and-on for more than a decade, and while a solution will eventually arrive, we have settled on using GPG instead for the following reasons:

1. Many of the other driver teams at MongoDB are using GPG to sign their product releases. Consistency with the other teams means that we can reuse existing tooling for our own product releases.
2. GPG is widely available and has existing tools and procedures for dealing with web of trust (though they are admittedly quite arcane and intimidating to the uninitiated, unfortunately).

Ultimately, most users do not bother to verify gems, and will not be impacted by our choice of GPG over RubyGems' native method.


## Documentation

High level documentation and usage examples are located
[here](https://www.mongodb.com/docs/ecosystem/drivers/ruby/).

API documentation for the most recent release can be found
[here](https://mongodb.com/docs/ruby-driver/current/api/).
To build API documentation for the master branch, check out the
repository locally and run `rake docs`.

High-level driver documentation including tutorials and the reference that were in the docs folder can now be found
at the docs-ruby repository, [here](https://github.com/mongodb/docs-ruby)

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
