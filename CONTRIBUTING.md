# Contributing to the MongoDB Ruby Driver

Thank you for your interest in contributing to the MongoDB Ruby driver.

We are building this software together and appreciate and encourage
contributions from the community.

JIRA Tickets
------------

The Ruby driver team uses MongoDB JIRA to schedule and track work.
To report a problem with the driver, please [create a new
issue](https://jira.mongodb.org/secure/CreateIssue!default.jspa) in the Ruby
JIRA project. A ticket is appreciated, though not necessary, when submitting
a pull request.

Please consult [JIRA issues](https://jira.mongodb.org/browse/RUBY)
for existing known issues in the driver.

Environment
-----------

We recommend using [rbenv](https://github.com/sstephenson/rbenv) to set up
the Ruby development and testing environments, though other tools like
[RVM](https://rvm.io/) will also work. The driver currently supports
MRI 1.9.3-2.6 and JRuby 9.1-9.2.

A MongoDB cluster is required to run the tests. Setup procedures and
recommendations for various clusters, as well as how to configure the
driver's test suite, are covered in the [spec
readme](https://github.com/mongodb/mongo-ruby-driver/blob/master/spec/README.md).

The driver is tested on [Evergreen](https://github.com/evergreen-ci/evergreen),
MongoDB's in house continuous integration platform. After a pull request
is created, one of the Ruby driver team engineers will schedule continous
integration builds on Evergreen.

Pull Requests
-------------

Pull requests should be made against the master (development) branch and
include relevant tests, if applicable. The Ruby driver team will backport
the changes to the stable branches, if needed.

Talk To Us
----------

We would love to hear from you. If you want to work on something or have
questions please reach out to us by creating a [question](https://jira.mongodb.org/secure/CreateIssue.jspa?pid=10005&issuetype=6)
in JIRA.
