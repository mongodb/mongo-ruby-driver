#!/bin/bash

set -e

if echo "$AUTH" |grep -q ^aws; then
  # Do not set -x as this will expose passwords in Evergreen logs
  set +x
else
  set -x
fi

. `dirname "$0"`/functions.sh

arch=`host_arch`

show_local_instructions

set_home
set_env_vars

setup_ruby

prepare_server $arch

install_mlaunch_pip

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its log available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

mongo_version=`echo $MONGODB_VERSION |tr -d .`
if test $mongo_version = latest; then
  mongo_version=44
fi

args="--setParameter enableTestCommands=1"
# diagnosticDataCollectionEnabled is a mongod-only parameter on server 3.2,
# and mlaunch does not support specifying mongod-only parameters:
# https://github.com/rueckstiess/mtools/issues/696
# Pass it to 3.4 and newer servers where it is accepted by all daemons.
if test $mongo_version -ge 34; then
  args="$args --setParameter diagnosticDataCollectionEnabled=false"
fi
uri_options=
if test "$TOPOLOGY" = replica-set; then
  args="$args --replicaset --name ruby-driver-rs --nodes 2 --arbiter"
  export HAVE_ARBITER=1
elif test "$TOPOLOGY" = sharded-cluster; then
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
elif test "$AUTH" = x509; then
  args="$args --auth --username bootstrap --password bootstrap"
elif echo "$AUTH" |grep -q ^aws; then
  args="$args --auth --username bootstrap --password bootstrap"
  args="$args --setParameter authenticationMechanisms=MONGODB-AWS,SCRAM-SHA-1,SCRAM-SHA-256"
  uri_options="$uri_options&authMechanism=MONGODB-AWS&authSource=\$external"
fi
if test "$SSL" = ssl; then
  args="$args --sslMode requireSSL"\
" --sslPEMKeyFile spec/support/certificates/server-second-level-bundle.pem"\
" --sslCAFile spec/support/certificates/ca.crt"\
" --sslClientCertificate spec/support/certificates/client.pem"

  if test "$AUTH" = x509; then
    client_pem=client-x509.pem
    uri_options="$uri_options&authMechanism=MONGODB-X509"
  elif echo $RVM_RUBY |grep -q jruby; then
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

# Docker forwards ports to the external interface, not to the loopback.
# Hence we must bind to all interfaces here.
if test -n "$BIND_ALL"; then
  args="$args --bind_ip_all"
fi

python -m mtools.mlaunch.mlaunch --dir "$dbdir" --binarypath "$BINDIR" $args

install_deps

which bundle
bundle --version

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
  ruby -Ilib -I.evergreen/lib -rserver_setup -e ServerSetup.new.setup_aws_auth

  hosts="`uri_escape $MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID`:`uri_escape $MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY`@$hosts"
elif test "$AUTH" = aws-assume-role; then
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
  ruby -Ispec -Ilib -I.evergreen/lib -rec2_setup -e Ec2Setup.new.run
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
  curl -fLo libmongocrypt-all.tar.gz "https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz"
  tar xf libmongocrypt-all.tar.gz

  export LIBMONGOCRYPT_PATH=`pwd`/rhel-70-64-bit/nocrypto/lib64/libmongocrypt.so
  test -f "$LIBMONGOCRYPT_PATH"
fi

export MONGODB_URI="mongodb://$hosts/?serverSelectionTimeoutMS=30000$uri_options"

set_fcv

echo Preparing the test suite
bundle exec rake spec:prepare

if test "$TOPOLOGY" = sharded-cluster && test $MONGODB_VERSION = 3.6; then
  # On 3.6 server the sessions collection is not immediately available,
  # wait for it to spring into existence
  bundle exec rake spec:wait_for_sessions
fi

export MONGODB_URI="mongodb://$hosts/?appName=test-suite$uri_options"
echo "Running tests"
if test -n "$TEST_CMD"; then
  eval $TEST_CMD
else
  bundle exec rake spec:ci
fi
test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

python -m mtools.mlaunch.mlaunch stop --dir "$dbdir"

exit ${test_status}
