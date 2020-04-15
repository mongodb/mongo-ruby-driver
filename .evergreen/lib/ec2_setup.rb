autoload :AwsUtils, 'support/aws_utils'
autoload :Utils, 'support/utils'

class Ec2Setup
  def assign_instance_profile
    opts = {
      region: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_REGION'),
      access_key_id: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
      secret_access_key: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
    }

    ip_arn = ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_INSTANCE_PROFILE_ARN')
    puts "Setting instance profile to #{ip_arn} on #{Utils.ec2_instance_id}"
    orchestrator = AwsUtils::Orchestrator.new(**opts)
    orchestrator.set_instance_profile(Utils.ec2_instance_id,
      instance_profile_name: nil,
      instance_profile_arn: ip_arn,
    )

    Utils.wait_for_instance_profile
  end

  def clear_instance_profile
    opts = {
      region: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_REGION'),
      access_key_id: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
      secret_access_key: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
    }

    puts "Clearing instance profile on #{Utils.ec2_instance_id}"
    orchestrator = AwsUtils::Orchestrator.new(**opts)
    orchestrator.clear_instance_profile(Utils.ec2_instance_id)

    Utils.wait_for_no_instance_profile
  end
end
