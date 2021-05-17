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
for var in MONGODB_VERSION RVM_RUBY \
  OCSP_ALGORITHM OCSP_STATUS OCSP_DELEGATE OCSP_MUST_STAPLE \
  OCSP_CONNECTIVITY OCSP_VERIFIER
do
  value="${!var}"
  if test -n "$value"; then
    params="$params $var=${!var}"
  fi
done

./.evergreen/test-on-docker -d ubuntu1604 $params
