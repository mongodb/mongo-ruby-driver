#!/bin/bash

set -e
# IMPORTANT: Don't set trace (-x) to avoid secrets showing up in the logs.
set +x

MRSS_ROOT=`dirname "$0"`/../spec/shared

. $MRSS_ROOT/shlib/distro.sh
. $MRSS_ROOT/shlib/set_env.sh
. $MRSS_ROOT/shlib/config.sh
. `dirname "$0"`/functions.sh
. `dirname "$0"`/functions-kerberos.sh
. `dirname "$0"`/functions-config.sh

arch=`host_distro`

show_local_instructions

set_env_vars
set_env_python
set_env_ruby

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

if test -n "$SASL_HOST"; then
  configure_for_external_kerberos
else
  configure_local_kerberos
fi
configure_kerberos_ip_addr

# To test authentication using the mongo shell, note that the host name
# must be uppercased when it is used in the username.
# The following call works when using the docker image:
# /opt/mongodb/bin/mongo --host $SASL_HOST --authenticationMechanism=GSSAPI \
#   --authenticationDatabase='$external' --username $SASL_USER@`echo $SASL_HOST |tr a-z A-Z`

echo "Install dependencies"
export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle_install

# need to build the native extension, since it doesn't seem to build correctly
# when installed via github.
curdir=`pwd`
cd `bundle info --path mongo_kerberos`

# unset the BUNDLE_GEMFILE variable so the mongo_kerberos rakefile doesn't
# get confused by it...
saved_gemfile=$BUNDLE_GEMFILE
unset BUNDLE_GEMFILE

bundle install
rake compile
cd $curdir
export BUNDLE_GEMFILE=$saved_gemfile

bundle list

export MONGO_RUBY_DRIVER_KERBEROS=1
export MONGO_RUBY_DRIVER_KERBEROS_INTEGRATION=1

if test -n "$TEST_CMD"; then
  eval $TEST_CMD
else
  echo "Running tests"
  bundle exec rspec spec/kerberos
fi
