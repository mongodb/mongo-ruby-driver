#!/bin/bash

set -e
# IMPORTANT: Don't set trace (-x) to avoid secrets showing up in the logs.
set +x

. `dirname "$0"`/functions.sh

# When running in Evergreen, credentials are written to this file.
# In Docker they are already in the environment and the file does not exist.
if test -f .env.private; then
  . ./.env.private
fi

# The AWS auth-related Evergreen variables are set the same way for most/all
# drivers. Therefore we don't want to change the variable names in order to
# transparently benefit from possible updates to these credentials in
# the future.
#
# At the same time, the chosen names do not cleanly map to our configurations,
# therefore to keep the rest of our test suite readable we perform the
# remapping in this file.

case "$AUTH" in
  aws-regular)
    export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID="`get_var IAM_AUTH_ECS_ACCOUNT`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY="`get_var IAM_AUTH_ECS_SECRET_ACCESS_KEY`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN="`get_var IAM_AUTH_ECS_ACCOUNT_ARN`"
    ;;

  aws-assume-role)
    export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID="`get_var IAM_AUTH_ASSUME_AWS_ACCOUNT`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY="`get_var IAM_AUTH_ASSUME_AWS_SECRET_ACCESS_KEY`"

    # This is the ARN provided in the AssumeRole request. It is different
    # from the ARN that the credentials returned by the AssumeRole request
    # resolve to.
    export MONGO_RUBY_DRIVER_AWS_AUTH_ASSUME_ROLE_ARN="`get_var IAM_AUTH_ASSUME_ROLE_NAME`"

    # This is the ARN that the credentials obtained by the AssumeRole
    # request resolve to. It is hardcoded in
    # https://github.com/mongodb-labs/drivers-evergreen-tools/blob/master/.evergreen/auth_aws/aws_e2e_assume_role.js
    # and is not given as an Evergreen variable.
    # Note: the asterisk at the end is manufactured by the server and not
    # obtained from STS. See https://jira.mongodb.org/browse/RUBY-2425.
    export MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN="arn:aws:sts::557821124784:assumed-role/authtest_user_assume_role/*"
    ;;

  aws-ec2)
    export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID="`get_var IAM_AUTH_EC2_INSTANCE_ACCOUNT`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY="`get_var IAM_AUTH_EC2_INSTANCE_SECRET_ACCESS_KEY`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_INSTANCE_PROFILE_ARN="`get_var IAM_AUTH_EC2_INSTANCE_PROFILE`"
    # Region is not specified in Evergreen but can be specified when
    # testing locally.
    export MONGO_RUBY_DRIVER_AWS_AUTH_REGION=${MONGO_RUBY_DRIVER_AWS_AUTH_REGION:=us-east-1}

    if test -z "$MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN"; then
      # This is the ARN that the credentials obtained via EC2 instance metadata
      # resolve to. It is hardcoded in
      # https://github.com/mongodb-labs/drivers-evergreen-tools/blob/master/.evergreen/auth_aws/aws_e2e_ec2.js
      # and is not given as an Evergreen variable.
      # If you are testing with a different AWS account, your user ARN will be
      # different. You can specify your ARN by populating the environment
      # variable manually.
      export MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN="arn:aws:sts::557821124784:assumed-role/authtest_instance_profile_role/*"
    fi

    export TEST_CMD=${TEST_CMD:=rspec spec/integration/aws*spec.rb spec/integration/client_construction_aws*spec.rb}
    ;;

  aws-ecs)
    export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID="`get_var IAM_AUTH_ECS_ACCOUNT`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY="`get_var IAM_AUTH_ECS_SECRET_ACCESS_KEY`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_ECS_CLUSTER_ARN="`get_var IAM_AUTH_ECS_CLUSTER`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_ECS_SECURITY_GROUP="`get_var IAM_AUTH_ECS_SECURITY_GROUP`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_ECS_SUBNETS="`get_var IAM_AUTH_ECS_SUBNET_A`,`get_var IAM_AUTH_ECS_SUBNET_B`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_ECS_TASK_DEFINITION_ARN="`get_var IAM_AUTH_ECS_TASK_DEFINITION`"
    # Region is not specified in Evergreen but can be specified when
    # testing locally.
    export MONGO_RUBY_DRIVER_AWS_AUTH_REGION=${MONGO_RUBY_DRIVER_AWS_AUTH_REGION:=us-east-1}

    if test -z "$MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN"; then
      # This is the ARN that the credentials obtained via ECS task metadata
      # resolve to. It is hardcoded in
      # https://github.com/mongodb-labs/drivers-evergreen-tools/blob/master/.evergreen/auth_aws/lib/ecs_hosted_test.js
      # and is not given as an Evergreen variable.
      # If you are testing with a different AWS account, your user ARN will be
      # different. You can specify your ARN by populating the environment
      # variable manually.
      export MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN="arn:aws:sts::557821124784:assumed-role/ecsTaskExecutionRole/*"
    fi

    export TEST_CMD=${TEST_CMD:=rspec spec/integration/aws*spec.rb spec/integration/client_construction_aws*spec.rb}
    exec `dirname $0`/run-tests-ecs.sh
    ;;

  aws-web-identity)
    cd `dirname "$0"`/auth_aws

    . ./activate_venv.sh
    export AWS_ACCESS_KEY_ID="`get_var IAM_AUTH_EC2_INSTANCE_ACCOUNT`"
    export AWS_SECRET_ACCESS_KEY="`get_var IAM_AUTH_EC2_INSTANCE_SECRET_ACCESS_KEY`"
    python -u lib/aws_unassign_instance_profile.py
    unset AWS_ACCESS_KEY_ID
    unset AWS_SECRET_ACCESS_KEY

    export IDP_ISSUER="`get_var IAM_WEB_IDENTITY_ISSUER`"
    export IDP_JWKS_URI="`get_var IAM_WEB_IDENTITY_JWKS_URI`"
    export IDP_RSA_KEY="`get_var IAM_WEB_IDENTITY_RSA_KEY`"
    export AWS_WEB_IDENTITY_TOKEN_FILE="`get_var IAM_WEB_IDENTITY_TOKEN_FILE`"
    python -u lib/aws_handle_oidc_creds.py token
    unset IDP_ISSUER
    unset IDP_JWKS_URI
    unset IDP_RSA_KEY

    cd -
    export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID="`get_var IAM_AUTH_EC2_INSTANCE_ACCOUNT`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY="`get_var IAM_AUTH_EC2_INSTANCE_SECRET_ACCESS_KEY`"
    export AWS_WEB_IDENTITY_TOKEN_FILE="`get_var IAM_WEB_IDENTITY_TOKEN_FILE`"
    export AWS_ROLE_ARN="`get_var IAM_AUTH_ASSUME_WEB_ROLE_NAME`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_ASSUME_ROLE_ARN="`get_var IAM_AUTH_ASSUME_WEB_ROLE_NAME`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN="arn:aws:sts::857654397073:assumed-role/webIdentityTestRole/*"

    export TEST_CMD=${TEST_CMD:=rspec spec/integration/aws*spec.rb spec/integration/client_construction_aws*spec.rb}
    ;;

  *)
    echo "Unknown AUTH value $AUTH" 1>&2
    exit 1
    ;;
esac

exec `dirname $0`/run-tests.sh
