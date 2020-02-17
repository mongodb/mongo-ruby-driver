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

set_home
set_env_vars

setup_ruby

arch=`host_arch`

prepare_server $arch

install_mlaunch_pip

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its log available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

args="--setParameter enableTestCommands=1"
if ! test "$MONGODB_VERSION" = 2.6 && ! test "$MONGODB_VERSION" = 3.0; then
  args="$args --setParameter diagnosticDataCollectionEnabled=false"
fi
uri_options=
if test "$TOPOLOGY" = replica_set; then
  args="$args --replicaset --name ruby-driver-rs"
  if test -z "$MMAPV1"; then
    args="$args --arbiter"
    export HAVE_ARBITER=1
  fi
elif test "$TOPOLOGY" = sharded_cluster; then
  args="$args --replicaset --sharded 2 --name ruby-driver-rs"
  if test -z "$SINGLE_MONGOS"; then
    args="$args --mongos 2"
  fi
else
  args="$args --single"
fi
if test -n "$MMAPV1"; then
  args="$args --storageEngine mmapv1"
  uri_options="$uri_options&retryReads=false&retryWrites=false"
fi
if test "$AUTH" = auth; then
  args="$args --auth --username bob --password pwd123"
fi
if test "$SSL" = ssl; then
  args="$args --sslMode requireSSL"\
" --sslPEMKeyFile spec/support/certificates/server-second-level-bundle.pem"\
" --sslCAFile spec/support/certificates/ca.crt"\
" --sslClientCertificate spec/support/certificates/client.pem"

  if echo $RVM_RUBY |grep -q jruby; then
    # JRuby does not grok chained certificate bundles -
    # https://github.com/jruby/jruby-openssl/issues/181
    client_pem=client.pem
  else
    client_pem=client-second-level-bundle.pem
  fi
  
  uri_options="$uri_options&"\
"tlsCAFile=spec/support/certificates/ca.crt&"\
"tlsCertificateKeyFile=spec/support/certificates/$client_pem"
fi

mlaunch --dir "$dbdir" --binarypath "$BINDIR" $args

install_deps

echo "Running specs"
which bundle
bundle --version

if test "$TOPOLOGY" = sharded_cluster; then
  if test -n "$SINGLE_MONGOS"; then
    # Some tests may run into https://jira.mongodb.org/browse/SERVER-16836
    # when executing against a multi-sharded mongos.
    # At the same time, due to pinning in sharded transactions,
    # it is beneficial to test a single shard to ensure that server
    # monitoring and selection are working correctly and recover the driver's
    # ability to operate in reasonable time after errors and fail points trigger
    # on a single shard
    echo Restricting to a single mongos
    hosts=localhost:27017
  else
    hosts=localhost:27017,localhost:27018
  fi
else
  hosts=localhost:27017
fi

if test "$AUTH" = auth; then
  hosts="bob:pwd123@$hosts"
fi

export MONGODB_URI="mongodb://$hosts/?serverSelectionTimeoutMS=30000$uri_options"

set_fcv

bundle exec rake spec:prepare

if test "$TOPOLOGY" = sharded_cluster && test $MONGODB_VERSION = 3.6; then
  # On 3.6 server the sessions collection is not immediately available,
  # wait for it to spring into existence
  bundle exec rake spec:wait_for_sessions
fi

export MONGODB_URI="mongodb://$hosts/?appName=test-suite$uri_options"
bundle exec rake spec:ci
test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

mlaunch stop --dir "$dbdir"

exit ${test_status}
