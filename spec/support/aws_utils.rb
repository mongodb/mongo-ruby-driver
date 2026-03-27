# frozen_string_literal: true

autoload :Byebug, 'byebug'
autoload :Paint, 'paint'

require 'aws-sdk-core'

module Aws
  autoload :CloudWatchLogs, 'aws-sdk-cloudwatchlogs'
  autoload :EC2, 'aws-sdk-ec2'
  autoload :ECS, 'aws-sdk-ecs'
  autoload :IAM, 'aws-sdk-iam'
  autoload :STS, 'aws-sdk-sts'
end

module AwsUtils
  NAMESPACE = 'mdb-ruby'

  AWS_AUTH_REGULAR_USER_NAME = "#{NAMESPACE}.aws-auth-regular"

  AWS_AUTH_ASSUME_ROLE_NAME = "#{NAMESPACE}.assume-role"

  AWS_AUTH_SECURITY_GROUP_NAME = "#{NAMESPACE}.ssh"

  AWS_AUTH_VPC_GATEWAY_NAME = NAMESPACE

  AWS_AUTH_VPC_SECURITY_GROUP_NAME = "#{NAMESPACE}.vpc-ssh"

  AWS_AUTH_VPC_CIDR = '10.42.142.64/28'

  AWS_AUTH_EC2_AMI_NAMES = {
    # https://wiki.debian.org/Cloud/AmazonEC2Image/Buster
    'debian10' => 'debian-10-amd64-20200210-166',
    'ubuntu1604' => 'ubuntu/images/hvm-ssd/ubuntu-xenial-16.04-amd64-server-20200317',
  }.freeze

  AWS_AUTH_EC2_INSTANCE_NAME = "#{NAMESPACE}.aws-auth)"

  AWS_AUTH_INSTANCE_PROFILE_NAME = "#{NAMESPACE}.ip"

  AWS_AUTH_ASSUME_ROLE_USER_POLICY_NAME = "#{NAMESPACE}.assume-role-user-policy"

  AWS_AUTH_EC2_ROLE_NAME = "#{NAMESPACE}.ec2-role"

  AWS_AUTH_ECS_CLUSTER_NAME = "#{NAMESPACE}_aws-auth"

  AWS_AUTH_ECS_TASK_FAMILY = "#{NAMESPACE}_aws-auth"

  AWS_AUTH_ECS_SERVICE_NAME = "#{NAMESPACE}_aws-auth"

  AWS_AUTH_ECS_LOG_GROUP = "/ecs/#{NAMESPACE}/aws-auth-ecs"

  AWS_AUTH_ECS_LOG_STREAM_PREFIX = 'task'

  # This role allows ECS tasks access to output logs to CloudWatch.
  AWS_AUTH_ECS_EXECUTION_ROLE_NAME = "#{NAMESPACE}.ecs-execution-role"

  # This role is assumed by ECS tasks.
  AWS_AUTH_ECS_TASK_ROLE_NAME = "#{NAMESPACE}.ecs-task-role"

  autoload :Base, 'support/aws_utils/base'
  autoload :Inspector, 'support/aws_utils/inspector'
  autoload :Orchestrator, 'support/aws_utils/orchestrator'
  autoload :Provisioner, 'support/aws_utils/provisioner'
end
