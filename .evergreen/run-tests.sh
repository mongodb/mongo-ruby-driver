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

if test -z "$PROJECT_DIRECTORY"; then
  PROJECT_DIRECTORY=`realpath $(dirname $0)/..`
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
set_env_ruby

# Make sure cmake is installed (in case we need to install the libmongocrypt
# helper)
if [ "$FLE" = "helper" ]; then
  install_cmake
fi

bundle_install

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
fi

if test "$API_VERSION_REQUIRED" = 1; then
  export SERVER_API='version: "1"'
fi

if ! test "$OCSP_VERIFIER" = 1 && ! test -n "$OCSP_CONNECTIVITY"; then
  echo Preparing the test suite
  bundle exec rake spec:prepare
fi

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
  export JRUBY_OPTS=-J-Xmx2g
  bundle exec rake spec:ci
fi

test_status=$?
echo "TEST STATUS: ${test_status}"
set -e

if test -f tmp/rspec-all.json; then
  mv tmp/rspec-all.json tmp/rspec.json
fi

kill_jruby || true

if test -n "$OCSP_MOCK_PID"; then
  kill "$OCSP_MOCK_PID"
fi

exit ${test_status}
