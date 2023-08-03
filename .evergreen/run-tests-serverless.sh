#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_python
set_env_ruby

bundle_install

export MONGODB_URI=`echo ${SERVERLESS_URI} | sed -r 's/mongodb\+srv:\/\//mongodb\+srv:\/\/'"${SERVERLESS_ATLAS_USER}"':'"${SERVERLESS_ATLAS_PASSWORD}@"'/g'`

export TOPOLOGY="load-balanced"

python3 -u .evergreen/mongodl.py --component crypt_shared -V ${SERVERLESS_MONGODB_VERSION} --out `pwd`/csfle_lib  --target `host_distro` || true
if test -f `pwd`/csfle_lib/lib/mongo_crypt_v1.so
then
    echo Usinn crypt shared library version ${SERVERLESS_MONGODB_VERSION}
    export MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH=`pwd`/csfle_lib/lib/mongo_crypt_v1.so
else
    echo Failed to download crypt shared library
    exit -1
fi

if ! ( test -f /etc/os-release & grep -q ^ID.*rhel /etc/os-release & grep -q ^VERSION_ID.*8.0 /etc/os-release ); then
    echo Serverless tests assume rhel80
    echo If this has changed, update .evergreen/run-tests-serverless.sh as necessary
    exit -1
fi

mkdir libmongocrypt
cd libmongocrypt
curl --retry 3 -fLo libmongocrypt-all.tar.gz "https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz"
tar xf libmongocrypt-all.tar.gz
# We assume that serverless tests always use rhel80
export LIBMONGOCRYPT_PATH=`pwd`/rhel-80-64-bit/nocrypto/lib64/libmongocrypt.so
cd -

cd .evergreen/csfle
. ./activate-kmstlsvenv.sh

pip install boto3~=1.19 'cryptography<3.4' pykmip~=0.10.0 'sqlalchemy<2.0.0'

python -u ./kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/server.pem --port 7999 &
python -u ./kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/expired.pem --port 8000 &
python -u ./kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/wrong-host.pem --port 8001 &
python -u ./kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/server.pem --port 8002 --require_client_cert &
python -u ./kms_kmip_server.py &

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
echo "Waiting for mock KMS servers to start... done."

# Obtain temporary AWS credentials
. ./set-temp-creds.sh
cd -

echo "Running specs"

bundle exec rspec \
    spec/spec_tests/client_side_encryption_spec.rb \
    spec/spec_tests/crud_spec.rb \
    spec/spec_tests/retryable_reads_spec.rb \
    spec/spec_tests/retryable_writes_spec.rb \
    spec/spec_tests/transactions_spec.rb \
    spec/spec_tests/change_streams_unified_spec.rb \
    spec/spec_tests/client_side_encryption_unified_spec.rb \
    spec/spec_tests/command_monitoring_unified_spec.rb \
    spec/spec_tests/crud_unified_spec.rb \
    spec/spec_tests/gridfs_unified_spec.rb \
    spec/spec_tests/retryable_reads_unified_spec.rb \
    spec/spec_tests/retryable_writes_unified_spec.rb \
    spec/spec_tests/sdam_unified_spec.rb \
    spec/spec_tests/sessions_unified_spec.rb \
    spec/spec_tests/transactions_unified_spec.rb

kill_jruby
# Terminate all kmip servers... and whatever else happens to be running
# that is a python script.
pkill python

exit ${test_status}
