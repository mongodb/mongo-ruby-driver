#!/bin/bash

set -e
set -o pipefail

MRSS_ROOT=`dirname "$0"`/../spec/shared

. $MRSS_ROOT/shlib/distro.sh
. $MRSS_ROOT/shlib/set_env.sh
. $MRSS_ROOT/shlib/server.sh
. $MRSS_ROOT/shlib/config.sh
. `dirname "$0"`/functions.sh
. `dirname "$0"`/functions-aws.sh
. `dirname "$0"`/functions-config.sh

arch=`host_distro`

set_home
set_env_vars
set_env_python
set_env_ruby

prepare_server $arch

install_mlaunch_venv

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its log available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

if test -z "$TOPOLOGY"; then
  export TOPOLOGY=standalone
fi

calculate_server_args
launch_server "$dbdir"

uri_options="$URI_OPTIONS"

bundle_install

if test "$TOPOLOGY" = sharded-cluster; then
  if test -n "$SINGLE_MONGOS"; then
    echo Restricting to a single mongos
    hosts=localhost:27017
  else
    hosts=localhost:27017,localhost:27018
  fi
elif test "$TOPOLOGY" = replica-set; then
  hosts=localhost:27017,localhost:27018
  uri_options="$uri_options&replicaSet=test-rs"
else
  hosts=localhost:27017
fi

hosts="bob:pwd123@$hosts"

if test -n "$EXTRA_URI_OPTIONS"; then
  uri_options="$uri_options&$EXTRA_URI_OPTIONS"
fi

export MONGODB_URI="mongodb://$hosts/?serverSelectionTimeoutMS=30000$uri_options"

set_fcv

if test "$TOPOLOGY" = replica-set && ! echo "$MONGODB_VERSION" |fgrep -q 2.6; then
  ruby -Ilib -I.evergreen/lib -rbundler/setup -rserver_setup -e ServerSetup.new.setup_tags
fi

if test "$API_VERSION_REQUIRED" = 1; then
  ruby -Ilib -I.evergreen/lib -rbundler/setup -rserver_setup -e ServerSetup.new.require_api_version
  export SERVER_API='version: "1"'
fi

if test "$TOPOLOGY" = sharded-cluster && test $MONGODB_VERSION = 3.6; then
  # On 3.6 server the sessions collection is not immediately available,
  # wait for it to spring into existence
  bundle exec rake spec:wait_for_sessions
fi

export MONGODB_URI="mongodb://$hosts/?appName=test-suite$uri_options"

echo "Running benchmarks"
bundle exec rake driver_bench

bm_status=$?
echo "BENCHMARK STATUS: ${bm_status}"

python3 -m mtools.mlaunch.mlaunch stop --dir "$dbdir"

exit ${test_status}
