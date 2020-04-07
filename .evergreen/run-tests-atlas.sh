#!/bin/bash

set -ex

. `dirname "$0"`/functions.sh

set_env_vars

setup_ruby

install_deps

echo "Running specs"

test_status=0
for uri in ATLAS_REPLICA_SET_URI ATLAS_SHARDED_URI ATLAS_FREE_TIER_URI \
  ATLAS_TLS11_URI ATLAS_TLS12_URI
do
  # ${!foo} syntax is bash specific:
  # https://stackoverflow.com/questions/14049057/bash-expand-variable-in-a-variable
  export ATLAS_URI="${!uri}"

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
