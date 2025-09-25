detected_distro=

host_distro() {
  if test -z "$detected_distro"; then
    detected_distro=`_detect_distro`
  fi
  echo "$detected_distro"
}

_detect_distro() {
  local distro
  distro=
  if test -f /etc/debian_version; then
    # Debian or Ubuntu
    if test "`uname -m`" = aarch64; then
      release=`lsb_release -rs |tr -d .`
      distro="ubuntu$release"-arm
    elif lsb_release -is |grep -q Debian; then
      release=`lsb_release -rs |tr -d .`
      # In docker, release is something like 9.11.
      # In evergreen, release is 9.2.
      release=`echo $release |sed -e 's/^9.*/92/'`
      distro="debian$release"
    elif lsb_release -is |grep -q Ubuntu; then
      if test "`uname -m`" = ppc64le; then
        release=`lsb_release -rs |tr -d .`
        distro="ubuntu$release-ppc"
      else
        release=`lsb_release -rs |tr -d .`
        distro="ubuntu$release"
      fi
    else
      echo 'Unknown Debian flavor' 1>&2
      exit 1
    fi
  elif lsb_release -is |grep -qi suse; then
    if test "`uname -m`" = s390x; then
      release=`lsb_release -rs |sed -e 's/\..*//'`
      distro="suse$release-s390x"
    else
      echo 'Unknown Suse arch' 1>&2
      exit 1
    fi
  elif test -f /etc/redhat-release; then
    # RHEL or CentOS
    if test "`uname -m`" = s390x; then
      distro=rhel72-s390x
    elif test "`uname -m`" = ppc64le; then
      distro=rhel71-ppc
    elif lsb_release >/dev/null 2>&1; then
      if lsb_release -is |grep -q RedHat; then
        release=`lsb_release -rs |tr -d .`
        distro="rhel$release"
      elif lsb_release -is |grep -q CentOS; then
        release=`lsb_release -rs |cut -c 1 |sed -e s/7/70/ -e s/6/62/ -e s/8/80/`
        distro="rhel$release"
      else
        echo 'Unknown RHEL flavor' 1>&2
        exit 1
      fi
    else
      echo lsb_release missing, using /etc/redhat-release 1>&2
      release=`grep -o 'release [0-9]' /etc/redhat-release |awk '{print $2}'`
      release=`echo $release |sed -e s/7/70/ -e s/6/62/ -e s/8/80/`
      distro=rhel$release
    fi
  elif test -f /etc/os-release; then
    name=`grep -o '^NAME=.*' /etc/os-release | awk -F '"' '{ print $2 }'`
    version=`grep -o '^VERSION=.*' /etc/os-release | awk -F '"' '{ print $2 }'`
    if test "$name" = "Amazon Linux"; then
      distro=amazon$version
    else
      cat /etc/os-release
      echo 'Unknown distro' 1>&2
      exit 1
    fi
  else
    lsb_release -a
    echo 'Unknown distro' 1>&2
    exit 1
  fi
  echo "Detected distro: $distro" 1>&2
  echo $distro
}
