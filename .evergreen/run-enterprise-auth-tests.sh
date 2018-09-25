#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

. `dirname "$0"`/functions.sh

set_env_vars

setup_ruby

echo "Setting krb5 config file"
touch ${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty
export KRB5_CONFIG=${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty

echo "Writing keytab"
echo ${KEYTAB_BASE64} | base64 --decode > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab

echo "Running kinit"
kinit -k -t ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab -p ${PRINCIPAL}

echo "Install dependencies"
export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle install --gemfile="$BUNDLE_GEMFILE"

echo "Running tests"
bundle exec rspec spec/enterprise_auth -fd
