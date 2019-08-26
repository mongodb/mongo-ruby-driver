#!/bin/bash

set -o errexit  # Exit the script with error if any of the commands fail

. `dirname "$0"`/functions.sh

set_env_vars
setup_ruby

export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle install --gemfile="$BUNDLE_GEMFILE"

unset CI

bundle exec rspec spec/spec_tests/uri_options_spec.rb
bundle exec rspec spec/spec_tests/connection_string_spec.rb 
bundle exec rspec spec/mongo/uri/srv_protocol_spec.rb 
bundle exec rspec spec/mongo/uri_spec.rb 