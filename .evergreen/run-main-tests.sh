#!/bin/bash

set -e
set -o pipefail

if echo "$AUTH" |grep -q ^aws; then
  # Do not set -x as this will expose passwords in Evergreen logs
  set +x
else
  set -x
fi

if test -z "$PROJECT_DIRECTORY"; then
  PROJECT_DIRECTORY=`realpath $(dirname $0)/..`
fi

MRSS_ROOT=`dirname "$0"`/../spec/shared
. $MRSS_ROOT/shlib/distro.sh
. `dirname "$0"`/functions.sh

arch=`host_distro`

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

if test "$TOPOLOGY" = replica_set; then
echo $MONGODB_URI
  bundle exec ruby -Ilib -I.evergreen/lib -rserver_setup -e ServerSetup.new.setup_tags
fi

bundle exec rake spec:ci
test_status=$?
echo "TEST STATUS: ${test_status}"
set -e

if test -f tmp/rspec-all.json; then
  mv tmp/rspec-all.json tmp/rspec.json
fi

exit ${test_status}