#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/../spec/shared/shlib/server.sh
. `dirname "$0"`/functions.sh

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

sudo apt-get -y install libyaml-dev cmake

bundle_install

echo "Running specs"
export MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH=${CRYPT_SHARED_LIB_PATH}
bundle exec rake spec:prepare
bundle exec rspec spec/integration/client_side_encryption/on_demand_gcp_credentials_spec.rb

exit ${test_status}
