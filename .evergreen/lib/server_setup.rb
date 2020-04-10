require 'mongo'

Mongo::Logger.logger.level = :WARN

class ServerSetup
  def setup_aws_auth
    arn = env!('MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN')
    puts "Adding AWS-mapped user #{arn}"
    create_aws_user(arn)

    puts 'Setup done'
  end

  private

  def create_aws_user(arn)
    bootstrap_client.use('$external').database.users.create(
      arn,
      roles: [{role: 'root', db: 'admin'}],
      write_concern: {w: :majority, wtimeout: 5000},
    )
  end

  def require_env_vars(vars)
    vars.each do |var|
      unless env?(var)
        raise "#{var} must be set in environment"
      end
    end
  end

  def env?(key)
    ENV[key] && !ENV[key].empty?
  end

  def env!(key)
    ENV[key].tap do |value|
      if value.nil? || value.empty?
        raise "Value for #{key} is required in environment"
      end
    end
  end

  def env_true?(key)
    %w(1 true yes).include?(ENV[key]&.downcase)
  end

  def bootstrap_client
    @bootstrap_client ||= Mongo::Client.new(%w(localhost),
      user: 'bootstrap', password: 'bootstrap',
    )
  end
end
