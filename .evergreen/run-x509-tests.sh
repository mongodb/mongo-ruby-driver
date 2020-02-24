#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

. `dirname "$0"`/functions.sh

arch=`host_arch`

show_local_instructions

set_fcv
set_env_vars

setup_ruby

install_deps

prepare_server $arch

#install_mlaunch_pip

install_mongo_manager

export dbdir="$MONGO_ORCHESTRATION_HOME"/db
mkdir -p "$dbdir"

(export PATH=$RUBIES_PREFIX/ruby-2.7/bin:$PATH &&
  mongo-manager init --dir "$dbdir" --bin-dir "$BINDIR" \
    --tls-mode requireTLS \
    --tls-certificate-key-file spec/support/certificates/server.pem \
    --tls-ca-file spec/support/certificates/ca.crt \
    --user bootstrap --password bootstrap \
    -- --setParameter enableTestCommands=1
)

create_user_cmd="`cat <<'EOT'
  db.getSiblingDB("$external").runCommand(
    {
      createUser: "C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost",
      roles: [
           { role: "dbAdminAnyDatabase", db: "admin" },
           { role: "readWriteAnyDatabase", db: "admin" },
           { role: "userAdminAnyDatabase", db: "admin" },
           { role: "clusterAdmin", db: "admin" },
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

export MONGODB_URI="mongodb://localhost:27017/?tls=true&"\
"tlsCAFile=spec/support/certificates/ca.crt&"\
"tlsCertificateKeyFile=spec/support/certificates/client-x509.pem&"\
"authMechanism=MONGODB-X509"

bundle exec rake

test_status=$?
echo "TEST STATUS"
echo ${test_status}

kill_jruby

mongo_manager_stop

exit ${test_status}
