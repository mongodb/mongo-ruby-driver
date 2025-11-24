#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
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

bundle_install

echo "Running specs"

test_status=0
export ATLAS_URI=$MONGODB_URI

if test -z "$ATLAS_URI"; then
	echo "The \$$uri environment variable was not set" 1>&2
	test_status=1
fi

bundle exec rspec spec/atlas \
  --format Rfc::Riff --format RspecJunitFormatter --out tmp/rspec.xml
