#!/bin/bash
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export GCPOIDC_DRIVERS_TAR_FILE=/tmp/mongo-ruby-driver.tgz
tar czf $GCPOIDC_DRIVERS_TAR_FILE .
export GCPOIDC_TEST_CMD="source ./secrets-export.sh drivers/gcpoidc && ENVIRONMENT=gcp RVM_RUBY=${RVM_RUBY} ./.evergreen/${TEST_SCRIPT}"
export PROJECT_DIRECTORY=$PROJECT_DIRECTORY
bash $DRIVERS_TOOLS/.evergreen/auth_oidc/gcp/run-driver-test.sh
