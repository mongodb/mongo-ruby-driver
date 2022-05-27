#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_python
set_env_ruby

bundle_install

export MONGODB_URI=`echo ${SERVERLESS_URI} | sed -r 's/mongodb\+srv:\/\//mongodb\+srv:\/\/'"${SERVERLESS_ATLAS_USER}"':'"${SERVERLESS_ATLAS_PASSWORD}@"'/g'`

export TOPOLOGY="load-balanced"

echo "Running specs"

bundle exec rspec \
    spec/spec_tests/crud_spec.rb \
    spec/spec_tests/crud_unified_spec.rb \
    spec/spec_tests/retryable_reads_spec.rb \
    spec/spec_tests/retryable_writes_spec.rb \
    spec/spec_tests/transactions_spec.rb \
    spec/spec_tests/transactions_unified_spec.rb

kill_jruby

exit ${test_status}
