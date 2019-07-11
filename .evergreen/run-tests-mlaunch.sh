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

setup_ruby

install_deps

arch=ubuntu1404
version=4.0.9
prepare_server $arch $version

install_mlaunch

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its long available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

args="--setParameter enableTestCommands=1"
args="$args --setParameter diagnosticDataCollectionEnabled=false"
uri_options=
if test "$TOPOLOGY" = replica_set; then
  args="$args --replicaset --name ruby-driver-rs"
elif test "$TOPOLOGY" = sharded_cluster; then
  args="$args --replicaset --sharded 2 --name ruby-driver-rs"
else
  args="$args --single"
fi
if test -n "$MMAPV1"; then
  args="$args --storageEngine mmapv1"
  uri_options="$uri_options&retryReads=false&retryWrites=false"
fi
mlaunch --dir "$dbdir" --binarypath "$BINDIR" $args

echo "Running specs"
which bundle
bundle --version

export MONGODB_URI="mongodb://localhost:27017/?serverSelectionTimeoutMS=30000$uri_options"
bundle exec rake spec:prepare

export MONGODB_URI="mongodb://localhost:27017/?appName=test-suite$uri_options"
bundle exec rake spec:ci
test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

mlaunch stop --dir "$dbdir"

exit ${test_status}
