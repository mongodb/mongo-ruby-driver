autoload :AwsUtils, 'support/aws_utils'
autoload :Utils, 'support/utils'

class Ec2Setup
  def run
    opts = {
      region: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_REGION'),
      access_key_id: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
      secret_access_key: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
    }

    orchestrator = AwsUtils::Orchestrator.new(**opts)
    orchestrator.set_instance_profile(Utils.ec2_instance_id,
      instance_profile_name: nil,
      instance_profile_arn: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_INSTANCE_PROFILE_ARN'),
    )
  end
end
