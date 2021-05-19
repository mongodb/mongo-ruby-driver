#!/bin/bash

set -e
set -o pipefail

if echo "$AUTH" |grep -q ^aws; then
  # Do not set -x as this will expose passwords in Evergreen logs
  set +x
else
  set -x
fi

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/../spec/shared/shlib/server.sh
. `dirname "$0"`/functions.sh
. `dirname "$0"`/functions-aws.sh
. `dirname "$0"`/functions-config.sh

arch=`host_distro`

show_local_instructions

set_home
set_env_vars
set_env_ruby

prepare_server $arch

install_mlaunch_virtualenv

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its log available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

calculate_server_args
launch_ocsp_mock
launch_server "$dbdir"

uri_options="$URI_OPTIONS"

bundle_install

if test "$TOPOLOGY" = sharded-cluster; then
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
elif test "$AUTH" = x509; then
  create_user_cmd="`cat <<'EOT'
    db.getSiblingDB("$external").runCommand(
      {
        createUser: "C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost",
        roles: [
             { role: "root", db: "admin" },
        ],
        writeConcern: { w: "majority" , wtimeout: 5000 },
      }
    )
EOT
  `"

  "$BINDIR"/mongo --tls \
    --tlsCAFile spec/support/certificates/ca.crt \
    --tlsCertificateKeyFile spec/support/certificates/client-x509.pem \
    -u bootstrap -p bootstrap \
    --eval "$create_user_cmd"
elif test "$AUTH" = aws-regular; then
  clear_instance_profile

  ruby -Ilib -I.evergreen/lib -rserver_setup -e ServerSetup.new.setup_aws_auth

  hosts="`uri_escape $MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID`:`uri_escape $MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY`@$hosts"
elif test "$AUTH" = aws-assume-role; then
  clear_instance_profile

  ./.evergreen/aws -a "$MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID" \
    -s "$MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY" \
    -r us-east-1 \
    assume-role "$MONGO_RUBY_DRIVER_AWS_AUTH_ASSUME_ROLE_ARN" >.env.private.gen
  eval `cat .env.private.gen`
  export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
  export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
  export MONGO_RUBY_DRIVER_AWS_AUTH_SESSION_TOKEN=$AWS_SESSION_TOKEN
  ruby -Ilib -I.evergreen/lib -rserver_setup -e ServerSetup.new.setup_aws_auth

  export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
  export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
  export AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN

  aws sts get-caller-identity

  hosts="`uri_escape $MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID`:`uri_escape $MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY`@$hosts"

  uri_options="$uri_options&"\
"authMechanismProperties=AWS_SESSION_TOKEN:`uri_escape $MONGO_RUBY_DRIVER_AWS_AUTH_SESSION_TOKEN`"
elif test "$AUTH" = aws-ec2; then
  ruby -Ilib -I.evergreen/lib -rserver_setup -e ServerSetup.new.setup_aws_auth

  # We need to assign an instance profile to the current instance, otherwise
  # since we don't place credentials into the environment the test suite
  # cannot connect to the MongoDB server while bootstrapping.
  # The EC2 credential retrieval tests clears the instance profile as part
  # of one of the tests.
  ruby -Ispec -Ilib -I.evergreen/lib -rec2_setup -e Ec2Setup.new.assign_instance_profile
elif test "$AUTH" = aws-ecs; then
  if test -z "$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"; then
    # drivers-evergreen-tools performs this operation in its ECS E2E tester.
    eval export `strings /proc/1/environ |grep ^AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`
  fi

  ruby -Ilib -I.evergreen/lib -rserver_setup -e ServerSetup.new.setup_aws_auth
elif test "$AUTH" = kerberos; then
  export MONGO_RUBY_DRIVER_KERBEROS=1
fi

if test -n "$FLE"; then
  curl --retry 3 -fLo libmongocrypt-all.tar.gz "https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz"
  tar xf libmongocrypt-all.tar.gz

  export LIBMONGOCRYPT_PATH=`pwd`/rhel-70-64-bit/nocrypto/lib64/libmongocrypt.so
  test -f "$LIBMONGOCRYPT_PATH"
fi

if test -n "$OCSP_CONNECTIVITY"; then
  # TODO Maybe OCSP_CONNECTIVITY=* should set SSL=ssl instead.
  uri_options="$uri_options&tls=true"
fi

if test -n "$EXTRA_URI_OPTIONS"; then
  uri_options="$uri_options&$EXTRA_URI_OPTIONS"
fi

export MONGODB_URI="mongodb://$hosts/?serverSelectionTimeoutMS=30000$uri_options"

if echo "$AUTH" |grep -q ^aws-assume-role; then
  $BINDIR/mongo "$MONGODB_URI" --eval 'db.runCommand({serverStatus: 1})' |wc
fi

set_fcv

if test "$TOPOLOGY" = replica-set && ! echo "$MONGODB_VERSION" |fgrep -q 2.6; then
  ruby -Ilib -I.evergreen/lib -rbundler/setup -rserver_setup -e ServerSetup.new.setup_tags
fi

if test "$API_VERSION_REQUIRED" = 1; then
  ruby -Ilib -I.evergreen/lib -rbundler/setup -rserver_setup -e ServerSetup.new.require_api_version
  export SERVER_API='version: "1"'
fi

if ! test "$OCSP_VERIFIER" = 1 && ! test -n "$OCSP_CONNECTIVITY"; then
  echo Preparing the test suite
  bundle exec rake spec:prepare
fi

if test "$TOPOLOGY" = sharded-cluster && test $MONGODB_VERSION = 3.6; then
  # On 3.6 server the sessions collection is not immediately available,
  # wait for it to spring into existence
  bundle exec rake spec:wait_for_sessions
fi

export MONGODB_URI="mongodb://$hosts/?appName=test-suite$uri_options"

# Compression is handled via an environment variable, convert to URI option
if test "$COMPRESSOR" = zlib && ! echo $MONGODB_URI |grep -q compressors=; then
  add_uri_option compressors=zlib
fi

if test "$COMPRESSOR" = snappy; then
  add_uri_option compressors=snappy
fi

if test "$COMPRESSOR" = zstd; then
  add_uri_option compressors=zstd
fi


echo "Running tests"
set +e
if test -n "$TEST_CMD"; then
  eval $TEST_CMD
elif test "$FORK" = 1; then
  bundle exec rspec spec/integration/fork*spec.rb spec/stress/fork*spec.rb
elif test "$STRESS" = 1; then
  bundle exec rspec spec/integration/fork*spec.rb spec/stress
elif test "$OCSP_VERIFIER" = 1; then
  bundle exec rspec spec/integration/ocsp_verifier_spec.rb
elif test -n "$OCSP_CONNECTIVITY"; then
  bundle exec rspec spec/integration/ocsp_connectivity_spec.rb
elif test "$SOLO" = 1; then
  for attempt in `seq 10`; do
    echo "Attempt $attempt"
    bundle exec rspec spec/solo/clean_exit_spec.rb 2>&1 |tee test.log
    if grep -qi 'segmentation fault' test.log; then
      echo 'Test failed - Ruby crashed' 1>&2
      exit 1
    fi
    if fgrep -i '[BUG]' test.log; then
      echo 'Test failed - Ruby complained about a bug' 1>&2
      exit 1
    fi
  done
else
  bundle exec rake spec:ci
fi

test_status=$?
echo "TEST STATUS: ${test_status}"
set -e

if test -f tmp/rspec-all.json; then
  mv tmp/rspec-all.json tmp/rspec.json
fi

kill_jruby

if test -n "$OCSP_MOCK_PID"; then
  kill "$OCSP_MOCK_PID"
fi

python -m mtools.mlaunch.mlaunch stop --dir "$dbdir"

exit ${test_status}
