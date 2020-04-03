#!/bin/bash

set -e
# Do not set -x as this will expose passwords in Evergreen logs
set +x

# When running in Evergreen, credentials are written to this file.
# In Docker they are already in the environment and the file does not exist.
if test -f .env.private; then
  . ./.env.private
fi

# The AWS auth-related Evergreen variables are set the same way for most/all
# drivers. Therefore we don't want to change the variable names in order to
# transparently benefit from possible updates to these credentials in
# the future.
#
# At the same time, the chosen names do not cleanly map to our configurations,
# therefore to keep the rest of our test suite readable we perform the
# remapping in this file.

get_var() {
  var=$1
  value=${!var}
  if test -z "$value"; then
    echo "Missing value for $var" 1>&2
    exit 1
  fi
  echo "$value"
}

case "$AUTH" in
  aws-regular)
    export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID="`get_var IAM_AUTH_ECS_ACCOUNT`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY="`get_var IAM_AUTH_ECS_SECRET_ACCESS_KEY`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN="`get_var IAM_AUTH_ECS_ACCOUNT_ARN`"
    ;;
    
  *)
    echo "Unknown AUTH value $AUTH" 1>&2
    exit 1
    ;;
esac

exec `dirname $0`/run-tests.sh
