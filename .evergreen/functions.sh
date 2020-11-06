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

detected_arch=

host_arch() {
  if test -z "$detected_arch"; then
    detected_arch=`_detect_arch`
  fi
  echo "$detected_arch"
}

_detect_arch() {
  local arch
  arch=
  if test -f /etc/debian_version; then
    # Debian or Ubuntu
    if test "`uname -m`" = aarch64; then
      arch=ubuntu1604-arm
    elif lsb_release -i |grep -q Debian; then
      release=`lsb_release -r |awk '{print $2}' |tr -d .`
      # In docker, release is something like 9.11.
      # In evergreen, release is 9.2.
      release=`echo $release |sed -e 's/^9.*/92/'`
      arch="debian$release"
    elif lsb_release -i |grep -q Ubuntu; then
      if test "`uname -m`" = ppc64le; then
        release=`lsb_release -r |awk '{print $2}' |tr -d .`
        arch="ubuntu$release-ppc"
      else
        release=`lsb_release -r |awk '{print $2}' |tr -d .`
        arch="ubuntu$release"
      fi
    else
      echo 'Unknown Debian flavor' 1>&2
      exit 1
    fi
  elif test -f /etc/redhat-release; then
    # RHEL or CentOS
    if test "`uname -m`" = s390x; then
      arch=rhel72-s390x
    elif test "`uname -m`" = ppc64le; then
      arch=rhel71-ppc
    elif lsb_release >/dev/null 2>&1; then
      if lsb_release -i |grep -q RedHat; then
        release=`lsb_release -r |awk '{print $2}' |tr -d .`
        arch="rhel$release"
      elif lsb_release -i |grep -q CentOS; then
        release=`lsb_release -r |awk '{print $2}' |cut -c 1 |sed -e s/7/70/ -e s/6/62/ -e s/8/80/`
        arch="rhel$release"
      else
        echo 'Unknown RHEL flavor' 1>&2
        exit 1
      fi
    else
      echo lsb_release missing, using /etc/redhat-release 1>&2
      release=`grep -o 'release [0-9]' /etc/redhat-release |awk '{print $2}'`
      release=`echo $release |sed -e s/7/70/ -e s/6/62/ -e s/8/80/`
      arch=rhel$release
    fi
  else
    echo 'Unknown distro' 1>&2
    exit 1
  fi
  echo "Detected arch: $arch" 1>&2
  echo $arch
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
    export BUNDLE_GEMFILE=gemfiles/bson_master.gemfile
  elif test "$COMPRESSOR" = snappy; then
    export BUNDLE_GEMFILE=gemfiles/snappy_compression.gemfile
  fi

  # rhel62 ships with Python 2.6
  if test -d /opt/python/2.7/bin; then
    export PATH=/opt/python/2.7/bin:$PATH
  fi
}

setup_ruby() {
  if test -z "$RVM_RUBY"; then
    echo "Empty RVM_RUBY, aborting"
    exit 2
  fi

  #ls -l /opt

  # Necessary for jruby
  # Use toolchain java if it exists
  if [ -f /opt/java/jdk8/bin/java ]; then
    export JAVACMD=/opt/java/jdk8/bin/java
    export PATH=$PATH:/opt/java/jdk8/bin
  fi

  # ppc64le has it in a different place
  if test -z "$JAVACMD" && [ -f /usr/lib/jvm/java-1.8.0/bin/java ]; then
    export JAVACMD=/usr/lib/jvm/java-1.8.0/bin/java
    export PATH=$PATH:/usr/lib/jvm/java-1.8.0/bin
  fi

  if [ "$RVM_RUBY" == "ruby-head" ]; then
    # When we use ruby-head, we do not install the Ruby toolchain.
    # But we still need Python 3.6+ to run mlaunch.
    # Since the ruby-head tests are run on ubuntu1604, we can use the
    # globally installed Python toolchain.
    #export PATH=/opt/python/3.7/bin:$PATH

    # 12.04, 14.04 and 16.04 are good
    curl -fLo ruby-head.tar.bz2 http://rubies.travis-ci.org/ubuntu/`lsb_release -rs`/x86_64/ruby-head.tar.bz2
    tar xf ruby-head.tar.bz2
    export PATH=`pwd`/ruby-head/bin:`pwd`/ruby-head/lib/ruby/gems/2.6.0/bin:$PATH
    ruby --version
    ruby --version |grep dev
  else
    if test "$USE_OPT_TOOLCHAIN" = 1; then
      # nothing, also PATH is already set
      :
    elif true; then

    # For testing toolchains:
    #toolchain_url=https://s3.amazonaws.com//mciuploads/mongo-ruby-toolchain/`host_arch`/f11598d091441ffc8d746aacfdc6c26741a3e629/mongo_ruby_driver_toolchain_`host_arch |tr - _`_patch_f11598d091441ffc8d746aacfdc6c26741a3e629_5e46f2793e8e866f36eda2c5_20_02_14_19_18_18.tar.gz
    toolchain_url=http://boxes.10gen.com/build/toolchain-drivers/mongo-ruby-driver/ruby-toolchain-`host_arch`-291ba4a4e8297f142796e70eee71b99f333e35e1.tar.xz
    curl --retry 3 -fL $toolchain_url |tar Jxf -
    export PATH=`pwd`/rubies/$RVM_RUBY/bin:$PATH
    #export PATH=`pwd`/rubies/python/3/bin:$PATH

    # Attempt to get bundler to report all errors - so far unsuccessful
    #curl -o bundler-openssl.diff https://github.com/bundler/bundler/compare/v2.0.1...p-mongo:report-errors.diff
    #find . -path \*/lib/bundler/fetcher.rb -exec patch {} bundler-openssl.diff \;

    else

    # Normal operation
    if ! test -d $HOME/.rubies/$RVM_RUBY/bin; then
      echo "Ruby directory does not exist: $HOME/.rubies/$RVM_RUBY/bin" 1>&2
      echo "Contents of /opt:" 1>&2
      ls -l /opt 1>&2 || true
      echo ".rubies symlink:" 1>&2
      ls -ld $HOME/.rubies 1>&2 || true
      echo "Our rubies:" 1>&2
      ls -l $HOME/.rubies 1>&2 || true
      exit 2
    fi
    export PATH=$HOME/.rubies/$RVM_RUBY/bin:$PATH

    fi

    ruby --version

    # Ensure we're using the right ruby
    ruby_name=`echo $RVM_RUBY |awk -F- '{print $1}'`
    ruby_version=`echo $RVM_RUBY |awk -F- '{print $2}' |cut -c 1-3`

    ruby -v |fgrep $ruby_name
    ruby -v |fgrep $ruby_version

    # We shouldn't need to update rubygems, and there is value in
    # testing on whatever rubygems came with each supported ruby version
    #echo 'updating rubygems'
    #gem update --system

    # Only install bundler when not using ruby-head.
    # ruby-head comes with bundler and gem complains
    # because installing bundler would overwrite the bundler binary.
    # We now install bundler in the toolchain, hence nothing needs to be done
    # in the tests.
    if false && echo "$RVM_RUBY" |grep -q jruby; then
      gem install bundler -v '<2'
    fi
  fi
}

bundle_install() {
  args=--quiet

  # On JRuby we can test against bson master but not in a conventional way.
  # See https://jira.mongodb.org/browse/RUBY-2156
  if echo $RVM_RUBY |grep -q jruby && test "$BSON" = master; then
    unset BUNDLE_GEMFILE
    git clone https://github.com/mongodb/bson-ruby
    (cd bson-ruby &&
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
  jruby_running=`ps -ef | grep 'jruby' | grep -v grep | awk '{print $2}'`
  if [ -n "$jruby_running" ];then
    echo "terminating remaining jruby processes"
    for pid in $(ps -ef | grep "jruby" | grep -v grep | awk '{print $2}'); do kill -9 $pid; done
  fi
}
