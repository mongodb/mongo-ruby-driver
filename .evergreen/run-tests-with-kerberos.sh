#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

. `dirname "$0"`/functions.sh

set_env_vars
setup_ruby

export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle_install

export MONGODB_URI='mongodb://localhost:27017'

bundle exec rake spec:prepare

bundle exec rspec spec/spec_tests/uri_options_spec.rb
bundle exec rspec spec/spec_tests/connection_string_spec.rb
bundle exec rspec spec/mongo/uri/srv_protocol_spec.rb
bundle exec rspec spec/mongo/uri_spec.rb
bundle exec rspec spec/integration/client_authentication_options_spec.rb
