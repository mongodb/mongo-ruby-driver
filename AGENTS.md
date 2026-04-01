# Project Description

This is the Ruby driver for the MongoDB database. It provides a Ruby interface for connecting to MongoDB, performing CRUD operations, and managing database connections. The API is standardized across all MongoDB drivers, via the specifications defined in the MongoDB Specifications repository: https://github.com/mongodb/specifications. The driver targets Ruby 2.7+. Do not use syntax or stdlib features unavailable in Ruby 2.7.

# Project Structure

The project is organized into the following directories:

- `lib/`: the main codebase
- `spec/`: RSpec tests for the project, and shared test data
- `bin/`: executable scripts
- `examples/`: example usage of the library
- `gemfiles/`: Gemfile files for different usage scenarios (primarily testing, aside from `standard.rb` which is used for development and production)
- `profile/`: profiling scripts and results


# Development Workflow

## Running tests

Tests require a running MongoDB instance. Set the URI via the `MONGODB_URI` environment variable:

```
MONGODB_URI="mongodb://localhost:27017,localhost:27018,localhost:27019/" bundle exec rspec spec/path/to/spec.rb
```

A replica set is typically available locally at `localhost:27017,27018,27019`.

## Linting

Run RuboCop after making changes, and always before committing:

```
bundle exec rubocop lib/mongo/changed_file.rb spec/mongo/changed_file_spec.rb
```

Pass the specific files you modified.

RuboCop is configured with performance, rake, and rspec plugins (`.rubocop.yml`).

## Commit convention

Prefix commit messages with the JIRA ticket: `RUBY-#### Short description`. The ticket number is typically in the branch name (e.g., branch `3795-foo` means `RUBY-3795`).

## Prose style

When writing prose — commit messages, code comments, documentation — be concise, write as a human would, avoid overly complicated sentences, and use no emojis.

## Definition of done

Always run the relevant spec file(s) against the local cluster before considering a task complete. Running tests is not optional. "Relevant" means: the spec file for each class you changed, plus any integration specs in `spec/integration/` that exercise the affected feature. If MongoDB is not reachable, report this to the user rather than trying to work around it.

## Thread and fiber safety

This driver runs in multi-threaded and multi-fiber environments. When writing or modifying code that touches connection pools, server monitors, or any shared state, always consider concurrent access. Use existing synchronization primitives in the codebase rather than introducing new ones.

## Spec fixtures

Unified test format YAML fixtures live in `spec/spec_tests/data/<suite_name>/`. To add new fixtures, copy YAML files from `specifications/source/<spec-name>/tests/unified/` into that directory. The runner loads all `*.yml` files automatically — no runner changes needed.

Do not write Ruby specs that duplicate behavior already covered by YAML spec tests. New Ruby specs should cover behavior that cannot be expressed in the unified test format.


# Code Reviews

See [.github/code-review.md](.github/code-review.md) for code review guidelines.
