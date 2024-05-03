#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_python
set_env_ruby

export MONGODB_URI=${MONGODB_URI}
export TEST_LAMBDA_DIRECTORY=`dirname "$0"`/../spec/faas/ruby-sam-app

. `dirname "$0"`/aws_lambda/run-deployed-lambda-aws-tests.sh
