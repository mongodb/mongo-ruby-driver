# This file contains basic functions common between all Ruby driver team
# projects: toolchain, bson-ruby, driver and Mongoid.

get_var() {
  var=$1
  value=${!var}
  if test -z "$value"; then
    echo "Missing value for $var" 1>&2
    exit 1
  fi
  echo "$value"
}

set_home() {
  if test -z "$HOME"; then
    export HOME=$(pwd)
  fi
}

uri_escape() {
  echo "$1" |ruby -rcgi -e 'puts CGI.escape(STDIN.read.strip).gsub("+", "%20")'
}

set_env_vars() {
  DRIVERS_TOOLS=${DRIVERS_TOOLS:-}

  if test -n "$AUTH"; then
    export ROOT_USER_NAME="bob"
    export ROOT_USER_PWD="pwd123"
  fi

  if test -n "$MONGODB_URI"; then
    export MONGODB_URI
  else
    unset MONGODB_URI
  fi

  export CI=1

  # JRUBY_OPTS were initially set for Mongoid
  export JRUBY_OPTS="-J-Xms512m -J-Xmx1536M"

  if test "$BSON" = min; then
    export BUNDLE_GEMFILE=gemfiles/bson_min.gemfile
  elif test "$BSON" = master; then
    export MONGO_RUBY_DRIVER_BSON_MASTER=1
    export BUNDLE_GEMFILE=gemfiles/bson_master.gemfile
  elif test "$BSON" = 4-stable; then
    export BUNDLE_GEMFILE=gemfiles/bson_4-stable.gemfile
  elif test "$COMPRESSOR" = snappy; then
    export BUNDLE_GEMFILE=gemfiles/snappy_compression.gemfile
  elif test "$COMPRESSOR" = zstd; then
    export BUNDLE_GEMFILE=gemfiles/zstd_compression.gemfile
  fi

  # rhel62 ships with Python 2.6
  if test -d /opt/python/2.7/bin; then
    export PATH=/opt/python/2.7/bin:$PATH
  fi
}

# Runs every Ruby process with YJIT when the installed ruby has it, as
# production deployments do. A ruby built without YJIT (JRuby, MRI older
# than 3.2) would only print a warning for RUBY_YJIT_ENABLE, so it is set
# only when YJIT is actually available. A value set by the caller (e.g.
# RUBY_YJIT_ENABLE=0 to run under the interpreter) is left alone.
enable_yjit() {
  if test -n "$RUBY_YJIT_ENABLE"; then
    echo "RUBY_YJIT_ENABLE=$RUBY_YJIT_ENABLE set by the caller"
  elif ruby --yjit -e 'exit(RubyVM::YJIT.enabled? ? 0 : 1)' >/dev/null 2>&1; then
    export RUBY_YJIT_ENABLE=1
    echo "YJIT enabled"
  else
    echo "YJIT not available in `ruby -v`"
  fi
}

bundle_install() {
  args=--quiet
  
  if test "$BSON" = master || test "$BSON" = 4-stable; then
    # In Docker bson is installed in the image, remove it if we need bson master.
    gem uni bson || true
  fi

  # On JRuby we can test against bson master but not in a conventional way.
  # See https://jira.mongodb.org/browse/RUBY-2156
  if echo $RVM_RUBY |grep -q jruby && (test "$BSON" = master || test "$BSON" = 4-stable); then
    unset BUNDLE_GEMFILE
    git clone https://github.com/mongodb/bson-ruby
    (cd bson-ruby &&
      git checkout "origin/$BSON" &&
      bundle install &&
      rake compile &&
      gem build *.gemspec &&
      gem install *.gem)

    # TODO redirect output of bundle install to file.
    # Then we don't have to see it in evergreen output.
    args=
  fi

  #which bundle
  #bundle --version
  if test -n "$BUNDLE_GEMFILE"; then
    args="$args --gemfile=$BUNDLE_GEMFILE"
  fi
  echo "Running bundle install $args"
  # Sometimes bundler fails for no apparent reason, run it again then.
  # The failures happen on both MRI and JRuby and have different manifestatinons.
  bundle install $args || bundle install $args
}

kill_jruby() {
  set +o pipefail
  jruby_running=`ps -ef | grep 'jruby' | grep -v grep | awk '{print $2}'`
  set -o pipefail
  if [ -n "$jruby_running" ];then
    echo "terminating remaining jruby processes"
    for pid in $jruby_running; do kill -9 $pid; done
  fi
}
