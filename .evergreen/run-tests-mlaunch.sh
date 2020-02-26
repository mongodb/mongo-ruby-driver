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

arch=`host_arch`

show_local_instructions

set_home
set_env_vars

setup_ruby

prepare_server $arch

install_mlaunch_git https://github.com/p-mongo/mtools wait-for-rs

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its log available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

mongo_version=`echo $MONGODB_VERSION |tr -d .`

args="--setParameter enableTestCommands=1"
# diagnosticDataCollectionEnabled is a mongod-only parameter on server 3.2,
# and mlaunch does not support specifying mongod-only parameters:
# https://github.com/rueckstiess/mtools/issues/696
# Pass it to 3.4 and newer servers where it is accepted by all daemons.
if test $mongo_version -ge 34; then
  args="$args --setParameter diagnosticDataCollectionEnabled=false"
fi
uri_options=
if test "$TOPOLOGY" = replica_set; then
  args="$args --replicaset --name ruby-driver-rs --nodes 2 --arbiter"
  export HAVE_ARBITER=1
elif test "$TOPOLOGY" = sharded_cluster; then
  args="$args --replicaset --nodes 1 --sharded 1 --name ruby-driver-rs"
  if test -z "$SINGLE_MONGOS"; then
    args="$args --mongos 2"
  fi
else
  args="$args --single"
fi
if test -n "$MMAPV1"; then
  args="$args --storageEngine mmapv1 --smallfiles --noprealloc"
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

# mlaunch frequently fails to provision sharded clusters with authentication -
# see https://github.com/rueckstiess/mtools/issues/691.
# Give it 5 attempts.
ok=false
for i in `seq 5`; do
  if mlaunch --dir "$dbdir" --binarypath "$BINDIR" $args; then
    ok=true
    break
  fi
  mlaunch stop --dir "$dbdir" || true
  rm -rf "$dbdir"
done

if ! $ok; then
  echo mlaunch failed to provision the desired deployment 1>&2
  exit 5
fi

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
if test -n "$TEST_CMD"; then
  eval $TEST_CMD
else
  bundle exec rake spec:ci
fi
test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

mlaunch stop --dir "$dbdir"

exit ${test_status}
