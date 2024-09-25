#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_python
set_env_ruby

bundle_install

ATLAS_URI=$MONGODB_URI \
  SERVERLESS=1 \
  EXAMPLE_TIMEOUT=600 \
  bundle exec rspec -fd spec/integration/search_indexes_prose_spec.rb \
    spec/spec_tests/index_management_unified_spec.rb

test_status=$?

kill_jruby

exit ${test_status}
