# Project Description

This is the Ruby driver for the MongoDB database. It provides a Ruby interface for connecting to MongoDB, performing CRUD operations, and managing database connections. The API is standardized across all MongoDB drivers, via the specifications defined in the MongoDB Specifications repository: https://github.com/mongodb/specifications.

# Project Structure

The project is organized into the following directories:

- `lib/`: the main codebase
- `spec/`: RSpec tests for the project, and shared test data
- `bin/`: executable scripts
- `examples/`: example usage of the library
- `gemfiles/`: Gemfile files for different usage scenarios (primarily testing, aside from `standard.rb` which is used for development and production)
- `profile/`: profiling scripts and results


# Code Reviews

When reviewing code, focus on:

## Security Critical Issues
- Check for hardcoded secrets, API keys, or credentials
- Check for instances of potential method call injection, dynamic code execution, symbol injection or other code injection vulnerabilities.

## Performance Red Flags
- Spot inefficient loops and algorithmic issues.
- Check for memory leaks and resource cleanup.

## Code Quality Essentials
- Methods should be focused and appropriately sized. If a method is doing too much, suggest refactorings to split it up.
- Use clear, descriptive naming conventions.
- Avoid encapsulation violations and ensure proper separation of concerns.
- All public classes, modules, and methods should have clear documentation in YARD format.
- If `method_missing` is implemented, ensure that `respond_to_missing?` is also implemented.
- Rubocop is used by this project to enforce code style. Always refer to the project's .rubocop.yml file for guidance on the project's style preferences.

## Driver-specific Concerns
- Look for code that might cause issues in a multi-threaded (or multi-fiber) environment.

## Review Style
- Be specific and actionable in feedback
- Explain the "why" behind recommendations
- Acknowledge good patterns when you see them
- Ask clarifying questions when code intent is unclear
- When possible, suggest that the pull request be labeled as a `bug`, a `feature`, or a `bcbreak` (a "backwards-compatibility break").
- PR's with no user-visible effect do not need to be labeled.
- Do not review YAML files under the `spec/` directory; these are test fixtures shared between all drivers.

Always prioritize security vulnerabilities and performance issues that could impact users.

Always suggest changes to improve readability and testability.

Be encouraging.

