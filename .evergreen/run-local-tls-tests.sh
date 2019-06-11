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

set_fcv
set_env_vars

setup_ruby

install_deps

arch=ubuntu1404
version=4.0.9
prepare_server $arch $version

install_mlaunch

# Launching mongod under $MONGO_ORCHESTRATION_HOME
# makes its long available through log collecting machinery

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"
mlaunch --dir "$dbdir" --binarypath "$BINDIR" --single \
  --sslMode requireSSL \
  --sslPEMKeyFile spec/support/certificates/server-second-level-bundle.pem \
  --sslCAFile spec/support/certificates/ca.crt \
  --sslClientCertificate spec/support/certificates/client.pem

echo "Running specs"
export MONGODB_URI="mongodb://localhost:27017/?tls=true&serverSelectionTimeoutMS=30000&"\
"tlsCAFile=spec/support/certificates/ca.crt&"\
"tlsCertificateKeyFile=spec/support/certificates/client-second-level-bundle.pem"
bundle exec rake spec:prepare

export MONGODB_URI="mongodb://localhost:27017/?tls=true&"\
"tlsCAFile=spec/support/certificates/ca.crt&"\
"tlsCertificateKeyFile=spec/support/certificates/client-second-level-bundle.pem"
bundle exec rspec spec/mongo/socket*
test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

mlaunch stop --dir "$dbdir"

exit ${test_status}
