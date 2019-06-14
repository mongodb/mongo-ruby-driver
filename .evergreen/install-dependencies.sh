#!/bin/sh

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Inspiration:
# https://github.com/mongodb/mongo-python-driver/blob/3.8.0/.evergreen/install-dependencies.sh#L5-L9

# Copy our test certificates over driver-evergreen-tools
cp ${PROJECT_DIRECTORY}/spec/support/certificates/client.crt \
  ${DRIVERS_TOOLS}/.evergreen/x509gen/client-public.pem
cp ${PROJECT_DIRECTORY}/spec/support/certificates/client.key \
  ${DRIVERS_TOOLS}/.evergreen/x509gen/client-private.pem
cp ${PROJECT_DIRECTORY}/spec/support/certificates/ca.crt \
  ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem
cp ${PROJECT_DIRECTORY}/spec/support/certificates/server-second-level-bundle.pem \
  ${DRIVERS_TOOLS}/.evergreen/x509gen/server.pem

# Replace MongoOrchestration's client certificate.
cp ${PROJECT_DIRECTORY}/spec/support/certificates/client.pem \
  ${MONGO_ORCHESTRATION_HOME}/lib/client.pem
