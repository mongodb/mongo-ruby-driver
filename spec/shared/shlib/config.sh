show_local_instructions_impl() {
  local arch="$1"
  shift
  
  echo To test this configuration locally:
  local params=
  while test -n "$1"; do
    key="$1"
    shift
    # ${!foo} syntax is bash specific:
    # https://stackoverflow.com/questions/14049057/bash-expand-variable-in-a-variable
    value="${!key}"
    if test -n "$value"; then
      params="$params $key=$value"
    fi
  done
  
  # $0 has the current script being executed which is also the script that
  # was initially invoked EXCEPT for the AWS configurations which use the
  # wrapper script.
  if echo "$AUTH" |grep -q ^aws; then
    script=.evergreen/run-tests-aws-auth.sh
  else
    script="$0"
  fi
  echo ./.evergreen/test-on-docker -d $arch $params -s "$script"
}
