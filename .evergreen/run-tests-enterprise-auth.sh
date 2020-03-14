#!/bin/bash

# IMPORTANT: Don't set trace (-x) to avoid secrets showing up in the logs.
set +x

set -e

. `dirname "$0"`/functions.sh

arch=`host_arch`

show_local_instructions

set_env_vars

setup_ruby

# Note that:
#
# 1. .env.private is supposed to be in Dotenv format which supports
#    multi-line values. Currently all values set for Kerberos tests are
#    single-line hence this isn't an issue.
#
# 2. The database for Kerberos is $external. This means the file cannot be
#    simply sourced into the shell, as that would expand $external as a
#    variable.
#
# To assign variables in a loop:
# https://unix.stackexchange.com/questions/348175/bash-scope-of-variables-in-a-for-loop-using-tee
#
# When running the tests via Docker, .env.private does not exist and instead
# all of the variables in it are written into the image (and are already
# available at this point).
if test -f ./.env.private; then
  while read line; do
    k=`echo "$line" |awk -F= '{print $1}'`
    v=`echo "$line" |awk -F= '{print $2}'`
    eval export $k="'"$v"'"
  done < <(cat ./.env.private)
fi

if test -z "$SASL_HOST"; then
  echo SASL_HOST must be set in the environment 1>&2
  exit 5
fi

# TODO Find out of $OS is set here, right now we only test on Linux thus
# it doesn't matter if it is set.
case "$OS" in
  cygwin*)
    IP_ADDR=`getent hosts ${SASL_HOST} | head -n 1 | awk '{print $1}'`
    ;;

  darwin)
    IP_ADDR=`dig ${SASL_HOST} +short | tail -1`
    ;;

  *)
    IP_ADDR=`getent hosts ${SASL_HOST} | head -n 1 | awk '{print $1}'`
esac

export IP_ADDR

echo "Setting krb5 config file"
touch ${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty
export KRB5_CONFIG=${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty

if test -z "$KEYTAB_BASE64"; then
  echo KEYTAB_BASE64 must be set in the environment 1>&2
  exit 5
fi

echo "Writing keytab"
echo "$KEYTAB_BASE64" | base64 --decode > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab

if test -z "$PRINCIPAL"; then
  echo PRINCIPAL must be set in the environment 1>&2
  exit 5
fi

echo "Running kinit"
kinit -k -t ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab -p "$PRINCIPAL"

# To test authentication using the mongo shell, note that the host name
# must be uppercased when it is used in the username.
# The following call works when using the docker image:
# /opt/mongodb/bin/mongo --host $SASL_HOST --authenticationMechanism=GSSAPI \
#   --authenticationDatabase='$external' --username $SASL_USER@`echo $SASL_HOST |tr a-z A-Z`

echo "Install dependencies"
export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle_install

export KERBEROS=1

echo "Running tests"
bundle exec rspec spec/enterprise_auth
