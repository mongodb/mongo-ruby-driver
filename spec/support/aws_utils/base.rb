# frozen_string_literal: true
# rubocop:todo all

module AwsUtils
  class Base

    def initialize(access_key_id: nil, secret_access_key: nil, region: nil, **options)
      @access_key_id = access_key_id || ENV['AWS_ACCESS_KEY_ID']
      @secret_access_key = secret_access_key || ENV['AWS_SECRET_ACCESS_KEY']
      @region = region || ENV['AWS_REGION']
      @options = options
    end

    attr_reader :access_key_id, :secret_access_key, :region, :options

    private

    def detect_object(resp, resp_attr, object_attr, value)
      resp.each do |batch|
        batch.send(resp_attr).each do |object|
          if object.send(object_attr) == value
            return object
          end
        end
      end
      nil
    end

    def ssh_security_group_id
      begin
        sg = ec2_client.describe_security_groups(
          group_names: [AWS_AUTH_SECURITY_GROUP_NAME],
        ).security_groups.first
        sg&.group_id
      rescue Aws::EC2::Errors::InvalidGroupNotFound
        # Unlike almost all other describe calls, this one raises an exception
        # if there isn't a security group matching the criteria.
        nil
      end
    end

    def ssh_security_group_id!
      ssh_security_group_id.tap do |security_group_id|
        if security_group_id.nil?
          raise 'Security group does not exist, please provision'
        end
      end
    end

    def ssh_vpc_security_group_id
      begin
        # If the top-level group_name parameter is used, only non-VPC
        # security groups are returned which does not find the VPC group
        # we are looking for here.
        sg = ec2_client.describe_security_groups(
          filters: [{
            name: 'group-name',
            values: [AWS_AUTH_VPC_SECURITY_GROUP_NAME],
          }],
        ).security_groups.first
        sg&.group_id
      rescue Aws::EC2::Errors::InvalidGroupNotFound
        # Unlike almost all other describe calls, this one raises an exception
        # if there isn't a security group matching the criteria.
        nil
      end
    end

    def ssh_vpc_security_group_id!
      ssh_vpc_security_group_id.tap do |security_group_id|
        if security_group_id.nil?
          raise 'Security group does not exist, please provision'
        end
      end
    end

    def subnet_id
      # This directly queries the subnets for the one with the expected
      # CIDR block, to save on the number of requests made to AWS.
      ec2_client.describe_subnets(
        filters: [{
          name: 'cidr-block',
          values: [AWS_AUTH_VPC_CIDR],
        }],
      ).subnets.first&.subnet_id
    end

    def subnet_id!
      subnet_id.tap do |subnet_id|
        if subnet_id.nil?
          raise 'Subnet does not exist, please provision'
        end
      end
    end

    def credentials
      Aws::Credentials.new(access_key_id, secret_access_key)
    end

    public

    def ec2_client
      @ec2_client ||= Aws::EC2::Client.new(
        region: region,
        credentials: credentials,
      )
    end

    def iam_client
      iam_client = Aws::IAM::Client.new(
        region: region,
        credentials: credentials,
      )
    end

    def ecs_client
      @ecs_client ||= Aws::ECS::Client.new(
        region: region,
        credentials: credentials,
      )
    end

    def logs_client
      @logs_client ||= Aws::CloudWatchLogs::Client.new(
        region: region,
        credentials: credentials,
      )
    end

    def sts_client
      @sts_client ||= Aws::STS::Client.new(
        region: region,
        credentials: credentials,
      )
    end
  end
end
