#!/bin/bash

# Note that mlaunch is executed with (and therefore installed with) Python 2.
# The reason for this is that in the past, some of the distros we tested on
# had an ancient version of Python 3 that was unusable (e.g. it couldn't
# install anything from PyPI due to outdated TLS/SSL implementation).
# It is likely that all of the current distros we use have a recent enough
# and working Python 3 implementation, such that we could use Python 3 for
# everything.
#
# Note that some distros (e.g. ubuntu2004) do not contain a `python' binary
# at all, thus python2 or python3 must be explicitly specified depending on
# the desired version.

set -e
set -o pipefail

if echo "$AUTH" |grep -q ^aws; then
  # Do not set -x as this will expose passwords in Evergreen logs
  set +x
else
  set -x
fi

MRSS_ROOT=`dirname "$0"`/../spec/shared

. $MRSS_ROOT/shlib/distro.sh
. $MRSS_ROOT/shlib/set_env.sh
. $MRSS_ROOT/shlib/server.sh
. $MRSS_ROOT/shlib/config.sh
. `dirname "$0"`/functions.sh
. `dirname "$0"`/functions-aws.sh
. `dirname "$0"`/functions-config.sh

arch=`host_distro`

show_local_instructions

set_home
set_env_vars
set_env_python
set_env_ruby

prepare_server $arch

if test "$DOCKER_PRELOAD" != 1; then
  install_mlaunch_venv
fi

# Make sure cmake is installed (in case we need to install the libmongocrypt
# helper)
if [ "$FLE" = "helper" ]; then
  install_cmake
fi

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its log available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

if test -z "$TOPOLOGY"; then
  export TOPOLOGY=standalone
fi

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
elif test "$TOPOLOGY" = replica-set; then
  # To set FCV we use mongo shell, it needs to be placed in replica set topology
  # or it can try to send the commands to secondaries.
  hosts=localhost:27017,localhost:27018
  uri_options="$uri_options&replicaSet=test-rs"
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
elif test "$AUTH" = aws-web-identity; then
  clear_instance_profile

  ruby -Ilib -I.evergreen/lib -rserver_setup -e ServerSetup.new.setup_aws_auth
elif test "$AUTH" = kerberos; then
  export MONGO_RUBY_DRIVER_KERBEROS=1
fi

if test -n "$FLE"; then
  # Downloading crypt shared lib
  if [ -z "$MONGO_CRYPT_SHARED_DOWNLOAD_URL" ]; then
    crypt_shared_version=${CRYPT_SHARED_VERSION:-$("${BINDIR}"/mongod --version | grep -oP 'db version v\K.*')}
    python3 -u .evergreen/mongodl.py --component crypt_shared -V ${crypt_shared_version} --out $(pwd)/csfle_lib  --target $(host_distro) || true
    if test -f $(pwd)/csfle_lib/lib/mongo_crypt_v1.so
    then
      export MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH=$(pwd)/csfle_lib/lib/mongo_crypt_v1.so
    else
      echo 'Could not find crypt_shared library'
    fi
  else
    echo "Downloading crypt_shared package from $MONGO_CRYPT_SHARED_DOWNLOAD_URL"
    mkdir -p $(pwd)/csfle_lib
    cd $(pwd)/csfle_lib
    curl --retry 3 -fL $MONGO_CRYPT_SHARED_DOWNLOAD_URL | tar zxf -
    export MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH=$(pwd)/lib/mongo_crypt_v1.so
    cd -
  fi

  # Start the KMS servers first so that they are launching while we are
  # fetching libmongocrypt.
  if test "$DOCKER_PRELOAD" != 1; then
    # We already have a virtualenv activated for mlaunch,
    # install kms dependencies into it.
    #. .evergreen/csfle/activate_venv.sh

    # Adjusted package versions:
    # cryptography 3.4 requires rust, see
    # https://github.com/pyca/cryptography/issues/5771.
    #pip install boto3~=1.19 cryptography~=3.4.8 pykmip~=0.10.0
    pip3 install boto3~=1.19 'cryptography<3.4' pykmip~=0.10.0 'sqlalchemy<2.0.0'
  fi
  python3 -u .evergreen/csfle/kms_http_server.py --ca_file .evergreen/x509gen/ca.pem --cert_file .evergreen/x509gen/server.pem --port 7999 &
  python3 -u .evergreen/csfle/kms_http_server.py --ca_file .evergreen/x509gen/ca.pem --cert_file .evergreen/x509gen/expired.pem --port 8000 &
  python3 -u .evergreen/csfle/kms_http_server.py --ca_file .evergreen/x509gen/ca.pem --cert_file .evergreen/x509gen/wrong-host.pem --port 8001 &
  python3 -u .evergreen/csfle/kms_http_server.py --ca_file .evergreen/x509gen/ca.pem --cert_file .evergreen/x509gen/server.pem --port 8002 --require_client_cert &
  python3 -u .evergreen/csfle/kms_kmip_server.py &
  python3 -u .evergreen/csfle/fake_azure.py &

  # Obtain temporary AWS credentials
  PYTHON=python3 . .evergreen/csfle/set-temp-creds.sh

  if test "$FLE" = helper; then
    echo "Using helper gem"
  elif test "$FLE" = path; then
    if false; then
      # We would ideally like to use the actual libmongocrypt binary here,
      # however there isn't a straightforward way to obtain a binary that
      # 1) is of a release version and 2) doesn't contain crypto.
      # These could be theoretically spelunked out of libmongocrypt's
      # evergreen tasks.
      curl --retry 3 -fLo libmongocrypt-all.tar.gz "https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz"
      tar xf libmongocrypt-all.tar.gz

      export LIBMONGOCRYPT_PATH=`pwd`/rhel-70-64-bit/nocrypto/lib64/libmongocrypt.so
    else
      # So, install the helper for the binary.
      gem install libmongocrypt-helper --pre

      # https://stackoverflow.com/questions/19072070/how-to-find-where-gem-files-are-installed
      path=$(find `gem env |grep INSTALLATION |awk -F: '{print $2}'` -name libmongocrypt.so |head -1 || true)
      if test -z "$path"; then
        echo Failed to find libmongocrypt.so in installed gems 1>&2
        exit 1
      fi
      cp $path .
      export LIBMONGOCRYPT_PATH=`pwd`/libmongocrypt.so

      gem uni libmongocrypt-helper
    fi
    test -f "$LIBMONGOCRYPT_PATH"
    ldd "$LIBMONGOCRYPT_PATH"
  else
    echo "Unknown FLE value: $FLE" 1>&2
    exit 1
  fi

  echo "Waiting for mock KMS servers to start..."
   wait_for_kms_server() {
      for i in $(seq 60); do
         if curl -s "localhost:$1"; test $? -ne 7; then
            return 0
         else
            sleep 1
         fi
      done
      echo "Could not detect mock KMS server on port $1"
      return 1
   }
   wait_for_kms_server 8000
   wait_for_kms_server 8001
   wait_for_kms_server 8002
   wait_for_kms_server 5698
   wait_for_kms_server 8080
   echo "Waiting for mock KMS servers to start... done."
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

python3 -m mtools.mlaunch.mlaunch stop --dir "$dbdir"

if test -n "$FLE" && test "$DOCKER_PRELOAD" != 1; then
  # Terminate all kmip servers... and whatever else happens to be running
  # that is a python script.
  pkill python3
fi

exit ${test_status}
