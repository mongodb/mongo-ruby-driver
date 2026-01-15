#!/usr/bin/env bash
# shellcheck shell=sh

# This file is no longer used directly by drivers-evergreen-tools.
# If using this file to download mongodb binaries, you should consider instead using `mongodl.py` and `mongosh-dl.py`.
# If using this file for get_distro, use `get-distro.sh`.
set -o errexit  # Exit the script with error if any of the commands fail

get_distro ()
{
   # shellcheck disable=SC3028
   _script_dir="$(dirname ${BASH_SOURCE:-$0})"
   . ${_script_dir}/get-distro.sh
}

# get_mongodb_download_url_for "linux-distro-version-architecture" "latest|44|42|40|36|34|32|30|28|26|24" "true|false"
# Sets EXTRACT to appropriate extract command
# Sets MONGODB_DOWNLOAD_URL to the appropriate download url
# Sets MONGO_CRYPT_SHARED_DOWNLOAD_URL to the corresponding URL to a crypt_shared library archive
get_mongodb_download_url_for ()
{
   _DISTRO=$1
   _VERSION=$2
   _DEBUG=$3

   EXTRACT="tar zxf"
   EXTRACT_MONGOSH=$EXTRACT

   case "$_DEBUG" in
      true)
         _component="archive-debug"
      ;;
      *)
         _component="archive"
      ;;
   esac

   case "$_DISTRO" in
      darwin-*)
         EXTRACT_MONGOSH="unzip -q"
      ;;
      windows32* | cygwin*-i686)
         EXTRACT="/cygdrive/c/Progra~1/7-Zip/7z.exe x"
         EXTRACT_MONGOSH="/cygdrive/c/Progra~1/7-Zip/7z.exe x"
      ;;
      windows64* | cygwin*-x86_64)
         EXTRACT="/cygdrive/c/Progra~2/7-Zip/7z.exe x"
         EXTRACT_MONGOSH="/cygdrive/c/Progra~2/7-Zip/7z.exe x"
      ;;
      # Windows on GitHub Actions
      mingw64_nt-*-x86_64)
         EXTRACT="7z.exe x"
         EXTRACT_MONGOSH="7z.exe x"
      ;;
   esac

   # Get the download url for the latest MongoSH.
   # shellcheck disable=SC3028
   _script_dir="$(dirname ${BASH_SOURCE:-$0})"
   _python3=$(bash -c ". $_script_dir/find-python3.sh && ensure_python3 2>/dev/null")
   MONGOSH_DOWNLOAD_URL=$($_python3 "${_script_dir}/mongosh_dl.py" --no-download | tr -d '\r')

   # Get the download url for MongoDB for the given version.
   MONGODB_DOWNLOAD_URL="$($_python3 "${_script_dir}/mongodl.py" --version $_VERSION --component $_component --no-download | tr -d '\r')"

   if [ -z "$MONGODB_DOWNLOAD_URL" ]; then
     echo "Unknown version: $_VERSION for $_DISTRO"
     exit 1
   fi

   MONGO_CRYPT_SHARED_DOWNLOAD_URL=$($_python3 "${_script_dir}/mongodl.py" --version $_VERSION --component crypt_shared --no-download | tr -d '\r')

   echo "$MONGODB_DOWNLOAD_URL"
}

# curl_retry emulates running curl with `--retry 5` and `--retry-all-errors`.
curl_retry ()
{
  for i in 1 2 4 8 16; do
    { curl --fail -sS --max-time 300 "$@" && return 0; } || sleep $i
  done
  return 1
}

# download_and_extract_package downloads a MongoDB server package.
download_and_extract_package ()
{
   MONGODB_DOWNLOAD_URL=$1
   EXTRACT=$2

   if [ -n "${MONGODB_BINARIES:-}" ]; then
      cd "$(dirname "$(dirname "${MONGODB_BINARIES:?}")")"
   else
      cd $DRIVERS_TOOLS
   fi

   echo "Installing server binaries..."
   curl_retry "$MONGODB_DOWNLOAD_URL" --output mongodb-binaries.tgz

   $EXTRACT mongodb-binaries.tgz
   echo "Installing server binaries... done."

   rm -f mongodb-binaries.tgz
   mv mongodb* mongodb
   chmod -R +x mongodb
   # Clear the environment to avoid "find: The environment is too large for exec()"
   # error on Windows.
   env -i PATH="$PATH" find . -name vcredist_x64.exe -exec {} /install /quiet \;
   echo "MongoDB server version: $(./mongodb/bin/mongod --version)"
   cd -
}

download_and_extract_mongosh ()
{
   MONGOSH_DOWNLOAD_URL=$1
   EXTRACT_MONGOSH=${2:-"tar zxf"}

   if [ -z "$MONGOSH_DOWNLOAD_URL" ]; then
      get_mongodb_download_url_for "$(get_distro)" latest false
   fi

   if [ -n "${MONGODB_BINARIES:-}" ]; then
      cd "$(dirname "$(dirname "${MONGODB_BINARIES:?}")")"
   else
      cd $DRIVERS_TOOLS
   fi

   echo "Installing MongoDB shell..."
   curl_retry $MONGOSH_DOWNLOAD_URL --output mongosh.tgz
   $EXTRACT_MONGOSH mongosh.tgz

   rm -f mongosh.tgz
   mv mongosh-* mongosh
   mkdir -p mongodb/bin
   mv mongosh/bin/* mongodb/bin
   rm -rf mongosh
   chmod -R +x mongodb/bin
   echo "Installing MongoDB shell... done."
   echo "MongoDB shell version: $(./mongodb/bin/mongosh --version)"
   cd -
}

# download_and_extract downloads a requested MongoDB server package.
# If the legacy shell is not included in the download, the legacy shell is also downloaded from the 5.0 package.
download_and_extract ()
{
   MONGODB_DOWNLOAD_URL=$1
   EXTRACT=$2
   MONGOSH_DOWNLOAD_URL=$3
   EXTRACT_MONGOSH=$4

   download_and_extract_package "$MONGODB_DOWNLOAD_URL" "$EXTRACT"

   if [ "$MONGOSH_DOWNLOAD_URL" ]; then
      download_and_extract_mongosh "$MONGOSH_DOWNLOAD_URL" "$EXTRACT_MONGOSH"
   fi

   if [ ! -z "${INSTALL_LEGACY_SHELL:-}" ] && [ ! -e $DRIVERS_TOOLS/mongodb/bin/mongo ] && [ ! -e $DRIVERS_TOOLS/mongodb/bin/mongo.exe ]; then
      # The legacy mongo shell is not included in server downloads of 6.0.0-rc6 or later. Refer: SERVER-64352.
      # Some test scripts use the mongo shell for setup.
      # Download 5.0 package to get the legacy mongo shell as a workaround until DRIVERS-2328 is addressed.
      echo "Legacy 'mongo' shell not detected."
      echo "Download legacy shell from 5.0 ... begin"
      # Use a subshell to avoid overwriting MONGODB_DOWNLOAD_URL and MONGO_CRYPT_SHARED_DOWNLOAD_URL.
      MONGODB50_DOWNLOAD_URL=$(
         get_mongodb_download_url_for "$DISTRO" "5.0" > /dev/null
         echo "$MONGODB_DOWNLOAD_URL"
      )

      SAVED_DRIVERS_TOOLS=$DRIVERS_TOOLS
      mkdir $DRIVERS_TOOLS/legacy-shell-download
      DRIVERS_TOOLS=$DRIVERS_TOOLS/legacy-shell-download
      download_and_extract_package "$MONGODB50_DOWNLOAD_URL" "$EXTRACT"
      if [ -e $DRIVERS_TOOLS/mongodb/bin/mongo ]; then
         cp $DRIVERS_TOOLS/mongodb/bin/mongo $SAVED_DRIVERS_TOOLS/mongodb/bin
      elif [ -e $DRIVERS_TOOLS/mongodb/bin/mongo.exe ]; then
         cp $DRIVERS_TOOLS/mongodb/bin/mongo.exe $SAVED_DRIVERS_TOOLS/mongodb/bin
      fi
      DRIVERS_TOOLS=$SAVED_DRIVERS_TOOLS
      rm -rf $DRIVERS_TOOLS/legacy-shell-download
      echo "Download legacy shell from 5.0 ... end"
   fi

   # Define SKIP_CRYPT_SHARED=1 to skip downloading crypt_shared. This is useful for platforms that have a
   # server release but don't ship a corresponding crypt_shared release, like Amazon 2018.
   if [ -z "${SKIP_CRYPT_SHARED:-}" ]; then
      if [ -z "$MONGO_CRYPT_SHARED_DOWNLOAD_URL" ]; then
         echo "There is no crypt_shared library for distro='$DISTRO' and version='$MONGODB_VERSION'".
      else
         echo "Downloading crypt_shared package from $MONGO_CRYPT_SHARED_DOWNLOAD_URL"
         download_and_extract_crypt_shared "$MONGO_CRYPT_SHARED_DOWNLOAD_URL" "$EXTRACT" CRYPT_SHARED_LIB_PATH
         echo "CRYPT_SHARED_LIB_PATH:" $CRYPT_SHARED_LIB_PATH
         if [ -z $CRYPT_SHARED_LIB_PATH ]; then
            echo "CRYPT_SHARED_LIB_PATH must be assigned, but wasn't" 1>&2 # write to stderr"
            exit 1
         fi
      fi
   fi
}

# download_and_extract_crypt_shared downloads and extracts a crypt_shared package into the current directory.
# Use get_mongodb_download_url_for to get a MONGO_CRYPT_SHARED_DOWNLOAD_URL.
download_and_extract_crypt_shared ()
{
   MONGO_CRYPT_SHARED_DOWNLOAD_URL=$1
   EXTRACT=$2
   __CRYPT_SHARED_LIB_PATH=${3:-CRYPT_SHARED_LIB_PATH}
   rm -rf crypt_shared_download
   mkdir crypt_shared_download
   cd crypt_shared_download

   curl_retry $MONGO_CRYPT_SHARED_DOWNLOAD_URL --output crypt_shared-binaries.tgz
   $EXTRACT crypt_shared-binaries.tgz

   LIBRARY_NAME="mongo_crypt_v1"
   # Windows package includes .dll in 'bin' directory.
   if [ -d ./bin ]; then
      cp bin/$LIBRARY_NAME.* ..
   else
      cp lib/$LIBRARY_NAME.* ..
   fi
   cd ..
   rm -rf crypt_shared_download

   RELATIVE_CRYPT_SHARED_LIB_PATH="$(find . -maxdepth 1 -type f \( -name "$LIBRARY_NAME.dll" -o -name "$LIBRARY_NAME.so" -o -name "$LIBRARY_NAME.dylib" \))"
   ABSOLUTE_CRYPT_SHARED_LIB_PATH=$(pwd)/$(basename $RELATIVE_CRYPT_SHARED_LIB_PATH)
   if [ "Windows_NT" = "$OS" ]; then
      # If we're on Windows, convert the "cygdrive" path to Windows-style paths.
      ABSOLUTE_CRYPT_SHARED_LIB_PATH=$(cygpath -m $ABSOLUTE_CRYPT_SHARED_LIB_PATH)
   fi
   eval $__CRYPT_SHARED_LIB_PATH=$ABSOLUTE_CRYPT_SHARED_LIB_PATH
}
