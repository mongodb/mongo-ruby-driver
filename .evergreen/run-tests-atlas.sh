#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_python
set_env_ruby

bundle_install

echo "Running specs"

test_status=0
for uri in ATLAS_REPLICA_SET_URI ATLAS_SHARDED_URI ATLAS_FREE_TIER_URI \
  ATLAS_TLS11_URI ATLAS_TLS12_URI ATLAS_SERVERLESS_URI ATLAS_SERVERLESS_LB_URI
do
  # ${!foo} syntax is bash specific:
  # https://stackoverflow.com/questions/14049057/bash-expand-variable-in-a-variable
  export ATLAS_URI="${!uri}"

  if test -z "$ATLAS_URI"; then
    echo "The \$$uri environment variable was not set" 1>&2
    test_status=1
  fi

  bundle exec rspec spec/atlas -fd
  this_test_status=$?
  echo "TEST STATUS"
  echo ${this_test_status}

  if test $this_test_status != 0; then
    test_status=$this_test_status
  fi
done

kill_jruby

exit ${test_status}
