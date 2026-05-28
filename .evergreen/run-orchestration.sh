#!/bin/bash
# Translates Ruby driver environment variables to drivers-tools format,
# then starts MongoDB via drivers-evergreen-tools run-mongodb.sh.

set -e

if test -z "${DRIVERS_TOOLS:-}"; then
  echo "DRIVERS_TOOLS must be set" >&2
  exit 1
fi

if test -z "${MONGO_ORCHESTRATION_HOME:-}"; then
  export MONGO_ORCHESTRATION_HOME="$DRIVERS_TOOLS/.evergreen/orchestration"
fi

# Translate topology names to orchestration format.
case "${TOPOLOGY:-server}" in
  replica-set-single-node)
    export TOPOLOGY=replica_set
    export ORCHESTRATION_FILE="${ORCHESTRATION_FILE:-single-node}"
    ;;
  standalone)
    export TOPOLOGY=server
    ;;
esac

# Single mongos: use a 1-router sharded cluster config.
if test "${SINGLE_MONGOS:-}" = 'true' && test "${TOPOLOGY:-}" = sharded_cluster; then
  export ORCHESTRATION_FILE="${ORCHESTRATION_FILE:-single-mongos}"
fi

# Load balancer support.
if test "${LOAD_BALANCED:-}" = 'true'; then
  export LOAD_BALANCER=1
fi

# x509 auth: start server with auth+ssl; the x509 user is created later in run-tests.sh.
if test "${AUTH:-}" = x509; then
  export AUTH=auth
  export SSL=yes
fi

# AWS auth: use the auth-aws orchestration file which enables MONGODB-AWS mechanism.
if echo "${AUTH:-}" | grep -q ^aws; then
  export AUTH_AWS=1
  export AUTH=auth
fi

# OCSP: select orchestration file based on algorithm and mustStaple flag.
# Without mustStaple the server does not staple, so use the disableStapling variant.
if test -n "${OCSP_ALGORITHM:-}"; then
  if test "${OCSP_MUST_STAPLE:-}" = 1; then
    _ocsp_file="${OCSP_ALGORITHM}-basic-tls-ocsp-mustStaple"
  else
    _ocsp_file="${OCSP_ALGORITHM}-basic-tls-ocsp-disableStapling"
  fi
  export ORCHESTRATION_FILE="${ORCHESTRATION_FILE:-$_ocsp_file}"
fi

# If prepare_server already downloaded MongoDB, reuse those binaries.
if test -n "${BINDIR:-}"; then
  export EXISTING_BINARIES_DIR="$BINDIR"
fi

# Copy Ruby-driver-specific orchestration configs that are not (yet) in drivers-evergreen-tools.
_configs_src="$(dirname "$0")/orchestration-configs"
_configs_dst="$MONGO_ORCHESTRATION_HOME/configs"
mkdir -p "$_configs_dst/replica_sets" "$_configs_dst/sharded_clusters"
cp "$_configs_src"/replica_sets/single-node.json "$_configs_dst/replica_sets/"
cp "$_configs_src"/replica_sets/single-node-ssl.json "$_configs_dst/replica_sets/"
cp "$_configs_src"/sharded_clusters/single-mongos.json "$_configs_dst/sharded_clusters/"

"$DRIVERS_TOOLS"/.evergreen/run-mongodb.sh start

# Export MONGODB_URI written by the orchestration tool.
. ./mo-expansion.sh
export MONGODB_URI
