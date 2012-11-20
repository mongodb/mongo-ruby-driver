## Contributing to the MongoDB Ruby Driver

Thank you for your interest in contributing to the MongoDB Ruby driver.

We are building this software together and strongly encourage contributions
from the community that are within the guidelines set forth below.

Bug Fixes and New Features
--------------------------

Before starting to write code, look for existing [tickets]
(https://jira.mongodb.org/browse/RUBY) or [create one]
(https://jira.mongodb.org/secure/CreateIssue!default.jspa) 
for your bug, issue, or feature request. This helps the community
avoid working on something that might not be of interest or which
has already been addressed.

Environment
-----------

We highly suggest using [RVM](https://rvm.io/) or [rbenv]
(https://github.com/sstephenson/rbenv) to set up Ruby development and
testing environments. In this way, moving between and testing code for
alternate Ruby versions (besides the one possibly included with your 
system) is simple. This practice is essential for ensuring the quality
of the driver.

Pull Requests
-------------

Pull requests should be made against the master (development)
branch and include relevant tests, if applicable. The driver follows
the Git-Flow branching model where the traditional master branch is
known as release and the master (default) branch is considered under
development.

Tests should pass under all Ruby interpreters which the MongoDB Ruby 
driver currently supports (1.8.7, 1.9.3, JRuby 1.6.x and 1.7.x) and will be 
automatically tested.

The results of pull request testing will be appended to the request.
If any tests do not pass, or relavant tests are not included the pull
request will not be considered.

Clusters and Replica Sets
-------------------------

If your bug fix or enhancement deals with Cluster or Replica Set
code, please run all relevant tests for those code subsets before
issuing the request.

* `rake test:sharded_cluster` for sharded clusters
* `rake test:replica_set` for replica sets

Cluster and Replica Set testing is currently **not** automatically
performed so it is important they are run in a thorough fashion under
all supported interpreters before a pull request is made.

Talk To Us
----------

We love to hear from you. If you want to work on something or have
questions / complaints please reach out to us by creating a [question]
(https://jira.mongodb.org/secure/CreateIssue.jspa?pid=10005&issuetype=6).
