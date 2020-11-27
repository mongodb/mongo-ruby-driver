# This file contains functions pertaining to downloading, starting and
# configuring a MongoDB server.

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

prepare_server() {
  arch=$1
  
  if test -n "$USE_OPT_MONGODB"; then
    export BINDIR=/opt/mongodb/bin
    export PATH=$BINDIR:$PATH
    return
  fi

  if test "$MONGODB_VERSION" = latest; then
    # Test on the most recent published 4.3 release.
    # https://jira.mongodb.org/browse/RUBY-1724
    echo 'Using "latest" server is not currently implemented' 1>&2
    exit 1
  else
    download_version="$MONGODB_VERSION"
  fi
  
  url=`$(dirname $0)/get-mongodb-download-url $download_version $arch`

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
  python2 -V || true
  # Current virtualenv fails with
  # https://github.com/pypa/virtualenv/issues/1630
  python -m pip install 'virtualenv<20' --user
  venvpath="$MONGO_ORCHESTRATION_HOME"/venv
  python2 -m virtualenv -p python2 $venvpath
  . $venvpath/bin/activate
  pip install 'mtools-legacy[mlaunch]'
}

install_mlaunch_pip() {
  if test -n "$USE_OPT_MONGODB" && which mlaunch >/dev/null 2>&1; then
    # mlaunch is preinstalled in the docker image, do not install it here
    return
  fi
  
  python -V || true
  python3 -V || true
  pythonpath="$MONGO_ORCHESTRATION_HOME"/python
  pip install -t "$pythonpath" 'mtools-legacy[mlaunch]'
  export PATH="$pythonpath/bin":$PATH
  export PYTHONPATH="$pythonpath"
}

install_mlaunch_git() {
  repo=$1
  branch=$2
  python -V || true
  python3 -V || true
  which pip || true
  which pip3 || true
  
  if false; then
    if ! virtualenv --version; then
      python3 `which pip3` install --user virtualenv
      export PATH=$HOME/.local/bin:$PATH
      virtualenv --version
    fi
    
    venvpath="$MONGO_ORCHESTRATION_HOME"/venv
    virtualenv -p python3 $venvpath
    . $venvpath/bin/activate
    
    pip3 install psutil pymongo
    
    git clone $repo mlaunch
    cd mlaunch
    git checkout origin/$branch
    python3 setup.py install
    cd ..
  else
    pip install --user 'virtualenv==13'
    export PATH=$HOME/.local/bin:$PATH
    
    venvpath="$MONGO_ORCHESTRATION_HOME"/venv
    virtualenv $venvpath
    . $venvpath/bin/activate
  
    pip install psutil pymongo
    
    git clone $repo mlaunch
    (cd mlaunch &&
      git checkout origin/$branch &&
      python setup.py install
    )
  fi
}
