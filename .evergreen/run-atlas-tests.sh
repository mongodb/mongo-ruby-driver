#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Values are: "auth" / "noauth" (default)
#       SSL                     Set to enable SSL. Values are "ssl" / "nossl" (default)
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       TOPOLOGY                Allows you to modify variables and the MONGODB_URI based on test topology
#                               Supported values: "server", "replica_set", "sharded_cluster"
#       RVM_RUBY                Define the Ruby version to test with, using its RVM identifier.
#                               For example: "ruby-2.3" or "jruby-9.1"
#       DRIVER_TOOLS            Path to driver tools.

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
TOPOLOGY=${TOPOLOGY:-server}
DRIVERS_TOOLS=${DRIVERS_TOOLS:-}


if [ "$AUTH" != "noauth" ]; then
  export ROOT_USER_NAME="bob"
  export ROOT_USER_PWD="pwd123"
fi
if [ "$COMPRESSOR" == "zlib" ]; then
  export COMPRESSOR="zlib"
fi
export CI=true


set +x
source ~/.rvm/scripts/rvm
set -x

# Necessary for jruby
export JAVACMD=/opt/java/jdk8/bin/java
export PATH=$PATH:/opt/java/jdk8/bin

if [ "$RVM_RUBY" == "ruby-head" ]; then
  rvm reinstall $RVM_RUBY
fi

export RVM_RUBY=2.5.0

# Don't errexit because this may call scripts which error
set +o errexit
set +x
rvm use $RVM_RUBY
set -x
set -o errexit

which ruby
export PATH=/opt/ruby-*/rvm/rubies/ruby-2.5.0/bin:$PATH
ruby --version

echo 'updating rubygems'
gem update --system

gem install bundler

echo "Installing all gem dependencies"
bundle install
bundle exec rake clean

env

echo "Running specs"
bundle exec rspec spec/atlas -fd
test_status=$?
echo "TEST STATUS"
echo ${test_status}

jruby_running=`ps -ef | grep 'jruby' | grep -v grep | awk '{print $2}'`
if [ -n "$jruby_running" ];then
  echo "terminating remaining jruby processes"
  for pid in $(ps -ef | grep "jruby" | grep -v grep | awk '{print $2}'); do kill -9 $pid; done
fi

exit ${test_status}
