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
      return 1
    fi
  elif test -f /etc/redhat-release; then
    # RHEL or CentOS
    if test "`uname -m`" = ppc64le; then
      arch=rhel71-ppc
    elif lsb_release -i |grep -q RedHat; then
      release=`lsb_release -r |awk '{print $2}' |tr -d .`
      arch="rhel$release"
    else
      echo 'Unknown RHEL flavor' 1>&2
      return 1
    fi
  else
    echo 'Unknown distro' 1>&2
    return 1
  fi
  echo "Detected arch: $arch" 1>&2
  echo $arch
}

set_home() {
  if test -z "$HOME"; then
    export HOME=$(pwd)
  fi
}

set_fcv() {
  if test -n "$FCV"; then
    mongo --eval 'assert.commandWorked(db.adminCommand( { setFeatureCompatibilityVersion: "'"$FCV"'" } ));' "$MONGODB_URI"
    mongo --quiet --eval 'db.adminCommand( { getParameter: 1, featureCompatibilityVersion: 1 } )' |grep  "version.*$FCV"
  fi
}

add_uri_option() {
  opt=$1
  
  if ! echo $MONGODB_URI |sed -e s,//,, |grep -q /; then
    MONGODB_URI="$MONGODB_URI/"
  fi
  
  if ! echo $MONGODB_URI |grep -q '?'; then
    MONGODB_URI="$MONGODB_URI?"
  fi
  
  MONGODB_URI="$MONGODB_URI&$opt"
}

set_env_vars() {
  AUTH=${AUTH:-noauth}
  SSL=${SSL:-nossl}
  MONGODB_URI=${MONGODB_URI:-}

  # drivers-evergreen-tools do not set tls parameter in URI when the
  # deployment uses TLS, repair this
  if test "$SSL" = ssl && ! echo $MONGODB_URI |grep -q tls=; then
    add_uri_option tls=true
  fi
  
  # Compression is handled via an environment variable, convert to URI option
  if test "$COMPRESSOR" = zlib && ! echo $MONGODB_URI |grep -q compressors=; then
    add_uri_option compressors=zlib
  fi

  TOPOLOGY=${TOPOLOGY:-server}
  DRIVERS_TOOLS=${DRIVERS_TOOLS:-}

  if [ "$AUTH" != "noauth" ]; then
    export ROOT_USER_NAME="bob"
    export ROOT_USER_PWD="pwd123"
  fi

  export MONGODB_URI

  export CI=evergreen

  # JRUBY_OPTS were initially set for Mongoid
  export JRUBY_OPTS="--server -J-Xms512m -J-Xmx2G"

  if test "$BSON" = min; then
    export BUNDLE_GEMFILE=gemfiles/bson_min.gemfile
  elif test "$BSON" = master; then
    export BUNDLE_GEMFILE=gemfiles/bson_master.gemfile
  fi
}

setup_ruby() {
  if test -z "$RVM_RUBY"; then
    echo "Empty RVM_RUBY, aborting"
    exit 2
  fi

  ls -l /opt

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
    # 12.04, 14.04 and 16.04 are good
    wget -O ruby-head.tar.bz2 http://rubies.travis-ci.org/ubuntu/`lsb_release -rs`/x86_64/ruby-head.tar.bz2
    tar xf ruby-head.tar.bz2
    export PATH=`pwd`/ruby-head/bin:`pwd`/ruby-head/lib/ruby/gems/2.6.0/bin:$PATH
    ruby --version
    ruby --version |grep dev

    #rvm reinstall $RVM_RUBY
  else
    if test "$USE_OPT_TOOLCHAIN" = 1; then
      # nothing, also PATH is already set
      :
    elif true; then

    # For testing toolchains:
    toolchain_url=https://s3.amazonaws.com//mciuploads/mongo-ruby-toolchain/`host_arch`/f11598d091441ffc8d746aacfdc6c26741a3e629/mongo_ruby_driver_toolchain_`host_arch |tr - _`_patch_f11598d091441ffc8d746aacfdc6c26741a3e629_5e46f2793e8e866f36eda2c5_20_02_14_19_18_18.tar.gz
    curl --retry 3 -fL $toolchain_url |tar zxf -
    export PATH=`pwd`/rubies/$RVM_RUBY/bin:`pwd`/rubies/python/3/bin:$PATH

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
    python - <<EOH
ruby = "${RVM_RUBY}".split("-")[0]
version = "${RVM_RUBY}".split("-")[1]
assert(ruby in "`ruby --version`")
assert(version in "`ruby --version`")
EOH

    # We shouldn't need to update rubygems, and there is value in
    # testing on whatever rubygems came with each supported ruby version
    #echo 'updating rubygems'
    #gem update --system

    # Only install bundler when not using ruby-head.
    # ruby-head comes with bundler and gem complains
    # because installing bundler would overwrite the bundler binary
    if echo "$RVM_RUBY" |grep -q jruby; then
      gem install bundler -v '<2'
    fi
  fi
}

bundle_install() {
  #which bundle
  #bundle --version
  args=--quiet
  if test -n "$BUNDLE_GEMFILE"; then
    args="$args --gemfile=$BUNDLE_GEMFILE"
  fi
  echo "Running bundle install $args"
  bundle install $args
}

install_deps() {
  bundle_install
  bundle exec rake clean
}

kill_jruby() {
  jruby_running=`ps -ef | grep 'jruby' | grep -v grep | awk '{print $2}'`
  if [ -n "$jruby_running" ];then
    echo "terminating remaining jruby processes"
    for pid in $(ps -ef | grep "jruby" | grep -v grep | awk '{print $2}'); do kill -9 $pid; done
  fi
}

prepare_server() {
  arch=$1
  version=$2

  url=http://downloads.10gen.com/linux/mongodb-linux-x86_64-enterprise-$arch-$version.tgz
  prepare_server_from_url $url
}

prepare_server_from_url() {
  url=$1

  mongodb_dir="$MONGO_ORCHESTRATION_HOME"/mdb
  mkdir -p "$mongodb_dir"
  curl --retry 3 $url |tar xz -C "$mongodb_dir" -f -
  BINDIR="$mongodb_dir"/`basename $url |sed -e s/.tgz//`/bin
  export PATH="$BINDIR":$PATH
}

install_mlaunch_virtualenv() {
  #export PATH=/opt/python/3.7/bin:$PATH
  python -V
  python3 -V
  #pip3 install --user virtualenv
  venvpath="$MONGO_ORCHESTRATION_HOME"/venv
  virtualenv -p python3 $venvpath
  . $venvpath/bin/activate
  pip install 'mtools[mlaunch]'
}

install_mlaunch_pip() {
  python -V
  python3 -V
  pythonpath="$MONGO_ORCHESTRATION_HOME"/python
  # The scripts in a python installation have shebangs pointing to the
  # prefix, which doesn't work for us because we unpack toolchain to a
  # different directory than prefix used for building. Work around this by
  # explicitly running pip3 with python.
  python3 `which pip3` install -t "$pythonpath" 'mtools[mlaunch]'
  export PATH="$pythonpath/bin":$PATH
  export PYTHONPATH="$pythonpath"
}
