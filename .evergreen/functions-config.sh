# This file contains functions pertaining to driver configuration in Evergreen.

show_local_instructions() {
  echo To test this configuration locally:
  params="MONGODB_VERSION=$MONGODB_VERSION TOPOLOGY=$TOPOLOGY RVM_RUBY=$RVM_RUBY STRESS=1"
  if test -n "$AUTH"; then
    params="$params AUTH=$AUTH"
  fi
  if test -n "$SSL"; then
    params="$params SSL=$SSL"
  fi
  if test -n "$COMPRESSOR"; then
    params="$params COMPRESSOR=$COMPRESSOR"
  fi
  if test -n "$FLE"; then
    params="$params FLE=$FLE"
  fi
  if test -n "$FCV"; then
    params="$params FCV=$FCV"
  fi
  if test -n "$MONGO_RUBY_DRIVER_LINT"; then
    params="$params MONGO_RUBY_DRIVER_LINT=$MONGO_RUBY_DRIVER_LINT"
  fi
  if test -n "$RETRY_READS"; then
    params="$params RETRY_READS=$RETRY_READS"
  fi
  if test -n "$RETRY_WRITES"; then
    params="$params RETRY_WRITES=$RETRY_WRITES"
  fi
  if test -n "$WITH_ACTIVE_SUPPORT"; then
    params="$params WITH_ACTIVE_SUPPORT=$WITH_ACTIVE_SUPPORT"
  fi
  if test -n "$SINGLE_MONGOS"; then
    params="$params SINGLE_MONGOS=$SINGLE_MONGOS"
  fi
  if test -n "$BSON"; then
    params="$params BSON=$BSON"
  fi
  if test -n "$MMAPV1"; then
    params="$params MMAPV1=$MMAPV1"
  fi
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
