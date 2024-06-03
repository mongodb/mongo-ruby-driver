#!/bin/bash
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export AZUREOIDC_DRIVERS_TAR_FILE=/tmp/mongo-ruby-driver.tgz
tar czf $AZUREOIDC_DRIVERS_TAR_FILE .
export AZUREOIDC_TEST_CMD="source ./env.sh && ENVIRONMENT=azure RVM_RUBY=${RVM_RUBY} ./.evergreen/${TEST_SCRIPT}"
export PROJECT_DIRECTORY=$PROJECT_DIRECTORY
bash $DRIVERS_TOOLS/.evergreen/auth_oidc/azure/run-driver-test.sh
