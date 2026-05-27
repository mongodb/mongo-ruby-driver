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

export ATLAS_TESTING=1

if test -f secrets-export.sh; then
  # shellcheck disable=SC1091
  . ./secrets-export.sh
  # Map from vault variable names (shared with Python/Node) to Ruby driver expected names.
  export ATLAS_REPLICA_SET_URI="${ATLAS_REPL}"
  export ATLAS_SHARDED_URI="${ATLAS_SHRD}"
  export ATLAS_FREE_TIER_URI="${ATLAS_FREE}"
  export ATLAS_TLS11_URI="${ATLAS_TLS11}"
  export ATLAS_TLS12_URI="${ATLAS_TLS12}"
  export ATLAS_X509_URI="${ATLAS_X509}"
  export ATLAS_X509_DEV_URI="${ATLAS_X509_DEV}"
fi

bundle exec rspec spec/atlas \
  --format Rfc::Riff --format RspecJunitFormatter --out tmp/rspec.xml
