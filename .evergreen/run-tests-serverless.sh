#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_ruby

bundle_install

SINGLE_HOST_URI=${SERVERLESS_URI%%,*}

if test -n "$SINGLE_MONGOS"; then
    export MONGODB_URI=`echo ${SINGLE_ATLASPROXY_SERVERLESS_URI} | sed -r 's/mongodb:\/\//mongodb:\/\/'"${SERVERLESS_ATLAS_USER}"':'"${SERVERLESS_ATLAS_PASSWORD}@"'/g'`
else
    export MONGODB_URI=`echo ${MULTI_ATLASPROXY_SERVERLESS_URI} | sed -r 's/mongodb:\/\//mongodb:\/\/'"${SERVERLESS_ATLAS_USER}"':'"${SERVERLESS_ATLAS_PASSWORD}@"'/g'`
fi

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
