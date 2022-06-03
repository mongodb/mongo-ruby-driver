#!/bin/bash

set -e
# IMPORTANT: Don't set trace (-x) to avoid secrets showing up in the logs.
set +x

MRSS_ROOT=`dirname "$0"`/../spec/shared

. $MRSS_ROOT/shlib/distro.sh
. $MRSS_ROOT/shlib/set_env.sh
. $MRSS_ROOT/shlib/config.sh
. `dirname "$0"`/functions.sh
. `dirname "$0"`/functions-config.sh

show_local_instructions

set_home
set_env_vars
set_env_python
set_env_ruby

bundle install --quiet

ruby -I.evergreen/lib -Ispec -recs_setup -e EcsSetup.new.run

eval `cat .env.private.ecs`

./.evergreen/provision-remote root@$PRIVATE_IP local

./.evergreen/test-remote root@$PRIVATE_IP \
  env AUTH=aws-ecs \
    RVM_RUBY=$RVM_RUBY MONGODB_VERSION=$MONGODB_VERSION \
    MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN="$MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN" \
    TOPOLOGY="$TOPOLOGY" \
    TEST_CMD="$TEST_CMD" .evergreen/run-tests.sh

mkdir -p tmp
scp root@$PRIVATE_IP:work/tmp/rspec.json tmp/
