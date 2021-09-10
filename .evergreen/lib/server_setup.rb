require 'mongo'

Mongo::Logger.logger.level = :WARN

class ServerSetup
  def setup_aws_auth
    arn = env!('MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN')
    puts "Adding AWS-mapped user #{wildcard_arn(arn)} for #{arn}"
    create_aws_user(arn)

    puts 'Setup done'
  end

  def setup_tags
    cfg = client.command(replSetGetConfig: 1).documents.first.fetch('config')
    members = cfg['members'].sort_by { |info| info['host'] }
    members.each_with_index do |member, index|
      # For case-sensitive tag set testing, add a mixed case tag.
      unless member['arbiterOnly']
        member['tags']['nodeIndex'] = index.to_s
      end
    end
    cfg['members'] = members
    cfg['version'] = cfg['version'] + 1
    client.command(replSetReconfig: cfg)
  end

  def require_api_version
    client.cluster.next_primary
    # In sharded clusters, the parameter must be set on each mongos.
    if Mongo::Cluster::Topology::Sharded === client.cluster.topology
      client.cluster.servers.each do |server|
        host = server.address.seed
        Mongo::Client.new([host], client.options.merge(connect: :direct)) do |c|
          c.command(setParameter: 1, requireApiVersion: true)
        end
      end
    else
      client.command(setParameter: 1, requireApiVersion: true)
    end
  end

  private

  # Creates an appropriate AWS mapped user for the provided ARN.
  #
  # The mapped user does not use the specified ARN directly but instead
  # uses a derived wildcard ARN. Because of this, multiple ARNs can map
  # to the same user.
  def create_aws_user(arn)
    bootstrap_client.use('$external').database.users.create(
      wildcard_arn(arn),
      roles: [{role: 'root', db: 'admin'}],
      write_concern: {w: :majority, wtimeout: 5000},
    )
  end

  def wildcard_arn(arn)
    if arn.start_with?('arn:aws:sts::')
      arn.sub(%r,/[^/]+\z,, '/*')
    else
      arn
    end
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

  def client
    @client ||= Mongo::Client.new(ENV.fetch('MONGODB_URI'))
  end

  def bootstrap_client
    @bootstrap_client ||= Mongo::Client.new(ENV['MONGODB_URI'] || %w(localhost),
      user: 'bootstrap', password: 'bootstrap', auth_mech: :scram, auth_mech_properties: nil,
    )
  end
end
