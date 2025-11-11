#!/bin/bash

set -ex

MRSS_ROOT=`dirname "$0"`/../spec/shared

. $MRSS_ROOT/shlib/distro.sh
. $MRSS_ROOT/shlib/set_env.sh
. $MRSS_ROOT/shlib/config.sh
. `dirname "$0"`/functions.sh
. `dirname "$0"`/functions-config.sh

arch=`host_distro`

show_local_instructions

set_env_vars
set_env_python


# Install rbenv and download the requested ruby version
rm -rf ~/.rbenv
git clone https://github.com/rbenv/rbenv.git ~/.rbenv
rm -rf ~/.rbenv/versions/
curl --retry 3 -fL http://boxes.10gen.com/build/toolchain-drivers/mongo-ruby-toolchain/library/`host_distro`/$RVM_RUBY.tar.xz |tar -xC $HOME/.rbenv/ -Jf -
export PATH="$HOME/.rbenv/bin:$PATH"
eval "$(rbenv init - bash)"
export FULL_RUBY_VERSION=$(ls ~/.rbenv/versions | head -n1)
rbenv global $FULL_RUBY_VERSION

export JAVA_HOME=/opt/java/jdk21

export JAVACMD=$JAVA_HOME/bin/java
export BUNDLE_GEMFILE=gemfiles/mongo_kerberos.gemfile
bundle_install

export MONGO_RUBY_DRIVER_KERBEROS=1

bundle exec rspec \
  spec/spec_tests/uri_options_spec.rb \
  spec/spec_tests/connection_string_spec.rb \
  spec/mongo/uri/srv_protocol_spec.rb \
  spec/mongo/uri_spec.rb \
  spec/integration/client_authentication_options_spec.rb
