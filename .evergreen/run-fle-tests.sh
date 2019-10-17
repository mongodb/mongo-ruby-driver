#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

. `dirname "$0"`/functions.sh

wget "https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz"
tar -xvf libmongocrypt-all.tar.gz

export LIBMONGOCRYPT_PATH=`pwd`/rhel-70-64-bit/lib64/libmongocrypt.so

set_env_vars
setup_ruby

install_deps

bundle exec rake

kill_jruby
