#!/bin/bash

# IMPORTANT: Don't set trace (-x) to avoid secrets showing up in the logs.
set +x

set -e

. `dirname "$0"`/functions.sh

set_env_vars

setup_ruby

# TODO Find out of $OS is set here, right now we only test on Linux thus
# it doesn't matter if it is set.
case "$OS" in
  cygwin*)
    IP_ADDR=`getent hosts ${sasl_host} | head -n 1 | awk '{print $1}'`
    ;;

  darwin)
    IP_ADDR=`dig ${sasl_host} +short | tail -1`
    ;;

  *)
    IP_ADDR=`getent hosts ${sasl_host} | head -n 1 | awk '{print $1}'`
esac

export IP_ADDR=$IP_ADDR

# Note that .env.private is supposed to be in Dotenv format which supports
# multi-line values. Currently all values set for Kerberos tests are
# single-line hence this isn't an issue.
. ./.env.private

echo "Setting krb5 config file"
touch ${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty
export KRB5_CONFIG=${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty

echo "Writing keytab"
echo ${KEYTAB_BASE64} | base64 --decode > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab

echo "Running kinit"
kinit -k -t ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab -p ${PRINCIPAL}

echo "Install dependencies"
export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle_install

export KERBEROS=1

echo "Running tests"
bundle exec rspec spec/enterprise_auth
