require 'singleton'

class ClusterConfig
  include Singleton

  def scanned_client
    $mongo_client ||= initialize_scanned_client!
  end

  def single_server?
    scanned_client.cluster.servers.length == 1
  end

  def server!
    server = scanned_client.cluster.servers.first
    if server.nil?
      raise ScannedClientHasNoServers
    end
    server
  end

  def mongos?
    server!.mongos?
  end

  def replica_set_name
    @replica_set_name ||= server!.replica_set_name
  end

  def server_version
    @server_version ||= begin
      client = ClientRegistry.instance.global_client('authorized')
      client.database.command(buildInfo: 1).first['version']
    end
  end

  def short_server_version
    server_version.split('.')[0..1].join('.')
  end

  def fcv
    @fcv ||= begin
      client = ClientRegistry.instance.global_client('root_authorized')
      rv = client.use(:admin).command(getParameter: 1, featureCompatibilityVersion: 1).first['featureCompatibilityVersion']
      rv['version'] || rv
    end
  end

  # Per https://jira.mongodb.org/browse/SERVER-39052, working with FCV
  # in sharded topologies is annoying. Also, FCV doesn't exist in servers
  # less than 3.4. This method returns FCV on 3.4+ servers when in single
  # or RS topologies, and otherwise returns the major.minor server version.
  def fcv_ish
    if server_version >= '3.4' && !mongos?
      fcv
    else
      short_server_version
    end
  end

  def primary_address
    @primary_address ||= begin
      client = ClientRegistry.instance.global_client('authorized')
      if client.cluster.topology.is_a?(Mongo::Cluster::Topology::ReplicaSetWithPrimary)
        client.cluster.servers.detect { |server| server.primary? }.address
      else
        client.cluster.servers.first.address
      end.seed
    end
  end

  # Try running a command on the admin database to see if the mongod was
  # started with auth.
  def auth_enabled?
    if @auth_enabled.nil?
      @auth_enabled = begin
        scanned_client.use(:admin).command(getCmdLineOpts: 1).first["argv"].include?("--auth")
      rescue => e
        e.message =~ /(not authorized)|(unauthorized)|(no users authenticated)|(requires authentication)/
      end
    end
    @auth_enabled
  end

  def topology
    @topology ||= begin
      topology = scanned_client.cluster.topology.class.name.sub(/.*::/, '')
      topology = topology.gsub(/([A-Z])/) { |match| '_' + match.downcase }.sub(/^_/, '')
      if topology =~ /^replica_set/
        topology = 'replica_set'
      end
      topology.to_sym
    end
  end
end
