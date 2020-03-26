determine_user() {
  user=`echo $target |awk -F@ '{print $1}'`
  if test -z "$user"; then
    user=`whoami`
  fi
  echo "$user"
}

do_ssh() {
  ssh -o StrictHostKeyChecking=no "$@"
}

do_rsync() {
  rsync -e "ssh -o StrictHostKeyChecking=no" "$@"
}
