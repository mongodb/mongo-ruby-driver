#!/bin/bash

set -ex

. `dirname "$0"`/functions.sh
. `dirname "$0"`/functions-config.sh

arch=`host_arch`

show_local_instructions

set_env_vars
setup_ruby

export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle_install

export MONGO_RUBY_DRIVER_KERBEROS=1

bundle exec rspec \
  spec/spec_tests/uri_options_spec.rb \
  spec/spec_tests/connection_string_spec.rb \
  spec/mongo/uri/srv_protocol_spec.rb \
  spec/mongo/uri_spec.rb \
  spec/integration/client_authentication_options_spec.rb
