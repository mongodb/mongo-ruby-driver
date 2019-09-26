#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

. `dirname "$0"`/functions.sh

set_fcv
set_env_vars

setup_ruby

install_deps

arch=ubuntu1404
version=4.0.9
prepare_server $arch $version

install_mlaunch

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"
mlaunch --dir "$dbdir" --binarypath "$BINDIR" --single \
  --sslMode requireSSL \
  --sslPEMKeyFile spec/support/certificates/server-second-level-bundle.pem \
  --sslCAFile spec/support/certificates/ca.crt \
  --sslClientCertificate spec/support/certificates/client-x509.pem

export MONGODB_URI="mongodb://localhost:27017/?tls=true&"\
"tlsCAFile=spec/support/certificates/ca.crt&"\
"tlsCertificateKeyFile=spec/support/certificates/client-x509.pem"
bundle exec rake spec:prepare

bundle exec rspec spec/integration/client_x509_spec.rb
test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

mlaunch stop --dir "$dbdir"

exit ${test_status}
