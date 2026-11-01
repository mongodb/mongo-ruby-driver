# Contributing to the MongoDB Ruby Driver

Thank you for your interest in contributing to the MongoDB Ruby driver.

We are building this software together and appreciate and encourage
contributions from the community.

Pull Requests
-------------

Pull requests should be made against the `master` branch and
include relevant tests, if applicable. The Ruby driver team will backport
the changes to the stable branches, if needed.

JIRA Tickets
------------

The Ruby driver team uses [MongoDB JIRA](https://jira.mongodb.org/browse/RUBY)
to schedule and track work.

A JIRA ticket is not required when submitting a pull request, but is
appreciated especially for non-trivial changes.

## Test coverage

The Evergreen `Coverage` buildvariant runs the spec suite with `COVERAGE=1` and
fails if any file's line coverage drops below the value recorded in
`.simplecov_baseline.json`.

To regenerate the baseline locally after an intentional coverage change (for
example, deleting tested code, or improving coverage and wanting to lock in
the gain):

```sh
COVERAGE=1 bundle exec rake spec:ci
bundle exec rake coverage:update_baseline
git add .simplecov_baseline.json
```

The baseline diff in your PR shows the reviewer per-file deltas.
