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

set_fcv
set_env_vars

#export DRIVER_TOOLS_CLIENT_CERT_PEM="${DRIVERS_TOOLS}/.evergreen/x509gen/client-public.pem"
#export DRIVER_TOOLS_CLIENT_KEY_PEM="${DRIVERS_TOOLS}/.evergreen/x509gen/client-private.pem"
#export DRIVER_TOOLS_CLIENT_CERT_KEY_PEM="${DRIVERS_TOOLS}/.evergreen/x509gen/client.pem"
#export DRIVER_TOOLS_CA_PEM="${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem"
#export DRIVER_TOOLS_CLIENT_KEY_ENCRYPTED_PEM="${DRIVERS_TOOLS}/.evergreen/x509gen/password_protected.pem"

if test -n "$SINGLE_MONGOS"; then
  # Some tests may run into https://jira.mongodb.org/browse/SERVER-16836
  # when executing against a multi-sharded mongos.
  # At the same time, due to pinning in sharded transactions,
  # it is beneficial to test a single shard to ensure that server
  # monitoring and selection are working correctly and recover the driver's
  # ability to operate in reasonable time after errors and fail points trigger
  # on a single shard
  echo Restricting to a single mongos
  export MONGODB_URI=`echo "$MONGODB_URI" |sed -e 's/,.*//'`
fi

setup_ruby

install_deps

echo "Running specs"
which bundle
bundle --version
bundle exec rake spec:prepare

bundle exec rspec spec/mongo/integration/connection_pool_stress_spec*
test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

exit ${test_status}
