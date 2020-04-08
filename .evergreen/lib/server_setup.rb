require 'mongo'

Mongo::Logger.logger.level = :WARN

class ServerSetup
  def setup_aws_auth
=begin
    require_env_vars(%w(
      MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID
      MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY
      MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN
    ))
=end

    arn = env!('MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN')
    puts "Adding AWS-mapped user #{arn}"
    create_aws_user(arn)

=begin The credentials to be used here need to be the temporary credentials
corresponding to the user being added, not credentials of the (administrative)
user that performed user creation a moment ago. In order for this to happen
this script must be passed two sets of credentials and the second set must be
obtained somehow. Instead of doing this we should simply start the server
initially without auth, add the AWS-mapped user, then restart the server with
auth such that there is never a bootstrap user to begin with.

    puts "Removing bootstrap user"
    aws_client = Mongo::Client.new(%w(localhost),
      user: env!('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
      password: env!('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
      auth_mech: :aws,
      auth_mech_properties: {'aws_session_token' => ENV['MONGO_RUBY_DRIVER_AWS_AUTH_SESSION_TOKEN']},
    )
    aws_client.use('admin').database.users.remove('bootstrap')
    aws_client.close

    puts "Verifying bootstrap user was removed"
    begin
      bootstrap_client['test-coll'].insert_one(test: true)
    rescue Mongo::Error::OperationFailure => e
      if e.message =~ /command insert requires authentication/
        # Expected outcome
      else
        raise
      end
    else
      raise 'Expected the bootstrap user to had been deleted, but it was not'
    end
=end

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
