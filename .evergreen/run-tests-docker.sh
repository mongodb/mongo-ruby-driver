#!/bin/bash

set -e
set -o pipefail

if echo "$AUTH" |grep -q ^aws; then
  # Do not set -x as this will expose passwords in Evergreen logs
  set +x
else
  set -x
fi

params=
for var in MONGODB_VERSION TOPOLOGY RVM_RUBY \
  OCSP_ALGORITHM OCSP_STATUS OCSP_DELEGATE OCSP_MUST_STAPLE \
  OCSP_CONNECTIVITY OCSP_VERIFIER FLE \
  AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION CRYPT_SHARED_VERSION MONGO_RUBY_DRIVER_AZURE_METADATA_HOST
do
  value="${!var}"
  if test -n "$value"; then
    params="$params $var=${!var}"
  fi
done

if test -f .env.private; then
  params="$params -a .env.private"
  gem install dotenv || gem install --user dotenv
fi

# OCSP verifier tests need debian10 so that ocsp mock works
./.evergreen/test-on-docker -p -d $DOCKER_DISTRO $params
