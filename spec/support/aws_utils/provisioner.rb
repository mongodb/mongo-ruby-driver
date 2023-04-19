# frozen_string_literal: true
# rubocop:todo all

module AwsUtils
  class Provisioner < Base

    def setup_aws_auth_resources
      security_group_id = ssh_security_group_id
      if security_group_id.nil?
        security_group_id = ec2_client.create_security_group(
          group_name: AWS_AUTH_SECURITY_GROUP_NAME,
          description: 'Inbound SSH',
        ).group_id
      end
      puts "EC2 Security group: #{security_group_id}"
      setup_security_group(security_group_id)

      vpc = ec2_client.describe_vpcs(
        filters: [{
          name: 'cidr',
          values: [AWS_AUTH_VPC_CIDR],
        }],
      ).vpcs.first
      if vpc.nil?
        vpc = ec2_client.create_vpc(
          cidr_block: AWS_AUTH_VPC_CIDR,
        ).vpc
      end

      # The VPC must have an internet gateway and the subnet in the VPC
      # must have a route to the internet gateway.
      # https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Internet_Gateway.html#d0e22943
      # Internet gateways cannot be named when they are created, therefore
      # we check if our VPC has a gateway and if not, create an unnamed one
      # and attach it right away.
      # https://aws.amazon.com/premiumsupport/knowledge-center/ecs-pull-container-error/
      igw = ec2_client.describe_internet_gateways(
        filters: [{
          name: 'attachment.vpc-id',
          values: [vpc.vpc_id],
        }],
      ).internet_gateways.first
      if igw.nil?
        igw = ec2_client.create_internet_gateway.internet_gateway
        ec2_client.attach_internet_gateway(
          internet_gateway_id: igw.internet_gateway_id,
          vpc_id: vpc.vpc_id,
        )
      end

      # https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Internet_Gateway.html#Add_IGW_Routing
      route_table = ec2_client.describe_route_tables(
        filters: [{
          name: 'vpc-id',
          values: [vpc.vpc_id],
        }],
      ).route_tables.first
      ec2_client.create_route(
        destination_cidr_block: '0.0.0.0/0',
        gateway_id: igw.internet_gateway_id,
        route_table_id: route_table.route_table_id,
      )

      vpc_security_group_id = ssh_vpc_security_group_id
      if vpc_security_group_id.nil?
        vpc_security_group_id = ec2_client.create_security_group(
          group_name: AWS_AUTH_VPC_SECURITY_GROUP_NAME,
          description: 'Inbound SSH',
          vpc_id: vpc.vpc_id,
        ).group_id
      end
      setup_security_group(vpc_security_group_id)

      subnet = ec2_client.describe_subnets(
        filters: [{
          name: 'vpc-id',
          values: [vpc.vpc_id],
        }],
      ).subnets.first
      if subnet.nil?
        subnet = ec2_client.create_subnet(
          cidr_block: AWS_AUTH_VPC_CIDR,
          vpc_id: vpc.vpc_id,
        ).subnet
      end
      puts "VPC: #{vpc.vpc_id}, subnet: #{subnet.subnet_id}, security group: #{vpc_security_group_id}"

      # For testing regular credentials, create an IAM user with no permissions.

      user = detect_object(iam_client.list_users, :users, :user_name, AWS_AUTH_REGULAR_USER_NAME)
      if user.nil?
        resp = iam_client.create_user(
          user_name: AWS_AUTH_REGULAR_USER_NAME,
        )
        user = resp.user
      end

      puts "Regular AWS auth unprivileged user: #{user.arn}"

      # Assume role testing
      # https://aws.amazon.com/premiumsupport/knowledge-center/iam-assume-role-cli/
      #
      # The instructions given in the above guide create an intermediate user
      # who has the ability to assume the role. This script reuses the
      # regular unprivileged user to be the user that assumes the role.
      user_policy = detect_object(iam_client.list_policies, :policies, :policy_name, AWS_AUTH_ASSUME_ROLE_USER_POLICY_NAME)
      if user_policy.nil?
        user_policy_document = {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "ec2:Describe*",
                "iam:ListRoles",
                "sts:AssumeRole",
              ],
              "Resource": "*",
            },
          ],
        }
        user_policy = iam_client.create_policy(
          policy_name: AWS_AUTH_ASSUME_ROLE_USER_POLICY_NAME,
          policy_document: user_policy_document.to_json,
        ).policy
      end

      iam_client.attach_user_policy(
        policy_arn: user_policy.arn,
        user_name: user.user_name,
      )

      assume_role = detect_object(iam_client.list_roles, :roles, :role_name, AWS_AUTH_ASSUME_ROLE_NAME)
      if assume_role.nil?
        aws_account_id = user.arn.split(':')[4]
        assume_role_policy = {
          "Version": "2012-10-17",
          "Statement": {
            "Effect": "Allow",
            "Principal": { "AWS": "arn:aws:iam::#{aws_account_id}:root" },
            "Action": "sts:AssumeRole",
          },
        }
        resp = iam_client.create_role(
          role_name: AWS_AUTH_ASSUME_ROLE_NAME,
          assume_role_policy_document: assume_role_policy.to_json,
          max_session_duration: 12*3600,
        )
        assume_role = resp.role
      end
      puts "Assume role ARN: #{assume_role.arn}"

      # For testing retrieval of credentials from EC2 link local endpoint,
      # create an instance profile.
      ips = iam_client.list_instance_profiles
      instance_profile = ips.instance_profiles.detect do |instance_profile|
        instance_profile.instance_profile_name == AWS_AUTH_INSTANCE_PROFILE_NAME
      end
      if instance_profile.nil?
        resp = iam_client.create_instance_profile(
          instance_profile_name: AWS_AUTH_INSTANCE_PROFILE_NAME,
        )
        instance_profile = resp.instance_profile
      end

      puts "EC2 instance profile: #{instance_profile.arn}"

      # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#create-iam-role
      assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": {
          "Effect": "Allow",
          "Principal": {"Service": "ec2.amazonaws.com"},
          "Action": "sts:AssumeRole",
        },
      }
      ec2_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "ec2:Describe*",
            ],
            "Resource": "*",
          },
        ],
      }
      ec2_role = create_role_with_policy(
        AWS_AUTH_EC2_ROLE_NAME,
        {
          assume_role_policy_document: assume_role_policy_document.to_json,
        },
        ec2_role_policy_document,
      )
      puts "EC2 role ARN: #{ec2_role.arn}"

      instance_profile.roles.each do |role|
        iam_client.remove_role_from_instance_profile(
          instance_profile_name: AWS_AUTH_INSTANCE_PROFILE_NAME,
          role_name: role.role_name,
        )
      end

      iam_client.add_role_to_instance_profile(
        instance_profile_name: AWS_AUTH_INSTANCE_PROFILE_NAME,
        role_name: AWS_AUTH_EC2_ROLE_NAME,
      )

      # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_AWSCLI_Fargate.html
      puts "ECS cluster name: #{AWS_AUTH_ECS_CLUSTER_NAME}"
      resp = ecs_client.describe_clusters(
        clusters: [AWS_AUTH_ECS_CLUSTER_NAME],
      )
      cluster = detect_object(resp, :clusters, :cluster_name, AWS_AUTH_ECS_CLUSTER_NAME)
      if cluster.nil?
        resp = ecs_client.create_cluster(
          cluster_name: AWS_AUTH_ECS_CLUSTER_NAME,
        )
        cluster = resp.cluster
      end

      # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
      ecs_assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
              "Service": "ecs-tasks.amazonaws.com",
            },
            "Action": "sts:AssumeRole",
          },
        ],
      }

      # The task role itself does not have any permissions.
      # The example given in https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
      # allows read-only access to an S3 bucket.
      ecs_task_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [],
      }
      ecs_task_role = create_role_with_policy(
        AWS_AUTH_ECS_TASK_ROLE_NAME,
        {
          assume_role_policy_document: ecs_assume_role_policy_document.to_json,
        },
      )

      # Logging to CloudWatch:
      # https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/QuickStartEC2Instance.html
      ecs_execution_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:DescribeLogStreams",
          ],
          "Resource": [
            "*"
          ],
        }],
      }
      ecs_execution_role = create_role_with_policy(
        AWS_AUTH_ECS_EXECUTION_ROLE_NAME,
        {
          assume_role_policy_document: ecs_assume_role_policy_document.to_json,
        },
        ecs_execution_role_policy_document,
      )

=begin
      iam_client.attach_role_policy(
        role_name: AWS_AUTH_ECS_ROLE_NAME,
        policy_arn: "arn:aws:iam::aws:policy/AmazonECSTaskExecutionRolePolicy",
      )
=end

      log_group = logs_client.describe_log_groups(
        log_group_name_prefix: AWS_AUTH_ECS_LOG_GROUP,
      ).log_groups.first
      unless log_group
        logs_client.create_log_group(
          log_group_name: AWS_AUTH_ECS_LOG_GROUP,
        )
      end

      logs_client.put_retention_policy(
        log_group_name: AWS_AUTH_ECS_LOG_GROUP,
        retention_in_days: 1,
      )
    end

    def reset_keys
      user = detect_object(iam_client.list_users, :users, :user_name, AWS_AUTH_REGULAR_USER_NAME)
      if user.nil?
        raise 'No user found, please run `aws setup-resources`'
      end

      iam_client.list_access_keys(
        user_name: user.user_name,
      ).to_h[:access_key_metadata].each do |access_key|
        iam_client.delete_access_key(
          user_name: user.user_name,
          access_key_id: access_key[:access_key_id],
        )
      end

      resp = iam_client.create_access_key(
        user_name: user.user_name,
      )
      access_key = resp.to_h[:access_key]

      puts "Credentials for regular user (#{AWS_AUTH_REGULAR_USER_NAME}):"
      puts "AWS_ACCESS_KEY_ID=#{access_key[:access_key_id]}"
      puts "AWS_SECRET_ACCESS_KEY=#{access_key[:secret_access_key]}"
      puts
    end

    private

    def create_role_with_policy(role_name, role_options, role_policy_document = nil)
      role = detect_object(iam_client.list_roles, :roles, :role_name, role_name)
      if role.nil?
        resp = iam_client.create_role({
          role_name: role_name,
        }.update(role_options))
        role = resp.role
      end

      if role_policy_document
        iam_client.put_role_policy(
          role_name: role_name,
          policy_name: "#{role_name}.policy",
          policy_document: role_policy_document.to_json,
        )
      end

      role
    end

    def setup_security_group(security_group_id)
      ec2_client.authorize_security_group_ingress(
        group_id: security_group_id,
        ip_permissions: [{
          from_port: 22,
          to_port: 22,
          ip_protocol: 'tcp',
          ip_ranges: [{
            cidr_ip: '0.0.0.0/0',
          }],
        }],
      )
    rescue Aws::EC2::Errors::InvalidPermissionDuplicate
    end
  end
end
