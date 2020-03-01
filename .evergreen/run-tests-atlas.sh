#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Values are: "auth" / "noauth" (default)
#       SSL                     Set to enable SSL. Values are "ssl" / "nossl" (default)
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       TOPOLOGY                Allows you to modify variables and the MONGODB_URI based on test topology
#                               Supported values: "server", "replica_set", "sharded_cluster"
#       RVM_RUBY                Define the Ruby version to test with, using its RVM identifier.
#                               For example: "ruby-2.3" or "jruby-9.1"
#       DRIVER_TOOLS            Path to driver tools.

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
