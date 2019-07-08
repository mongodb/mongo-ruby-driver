require 'singleton'

class ClusterConfig
  include Singleton

  def single_server?
    determine_cluster_config
    @single_server
  end

  def replica_set_name
    determine_cluster_config
    @replica_set_name
  end

  def server_version
    determine_cluster_config
    @server_version
  end

  def short_server_version
    server_version.split('.')[0..1].join('.')
  end

  def fcv
    determine_cluster_config
    @fcv
  end

  # Per https://jira.mongodb.org/browse/SERVER-39052, working with FCV
  # in sharded topologies is annoying. Also, FCV doesn't exist in servers
  # less than 3.4. This method returns FCV on 3.4+ servers when in single
  # or RS topologies, and otherwise returns the major.minor server version.
  def fcv_ish
    if server_version >= '3.4' && topology != :sharded
      fcv
    else
      if short_server_version == '4.1'
        '4.2'
      else
        short_server_version
      end
    end
  end

  def primary_address_str
    determine_cluster_config
    @primary_address.seed
  end

  def primary_description
    determine_cluster_config
    @primary_description
  end

  # Try running a command on the admin database to see if the mongod was
  # started with auth.
  def auth_enabled?
    if @auth_enabled.nil?
      @auth_enabled = begin
        basic_client.use(:admin).command(getCmdLineOpts: 1).first["argv"].include?("--auth")
      rescue => e
        e.message =~ /(not authorized)|(unauthorized)|(no users authenticated)|(requires authentication)/
      end
    end
    @auth_enabled
  end

  def topology
    determine_cluster_config
    @topology
  end

  private

  def determine_cluster_config
    return if @primary_address

    # Run all commands to figure out the cluster configuration from the same
    # client. This is somewhat wasteful when running a single test, but reduces
    # test runtime for the suite overall because all commands are sent on the
    # same connection rather than each command connecting to the cluster by
    # itself.
    client = ClientRegistry.instance.global_client('root_authorized')

    @server_version = client.database.command(buildInfo: 1).first['version']

    rv = client.use(:admin).command(getParameter: 1, featureCompatibilityVersion: 1).first['featureCompatibilityVersion']
    @fcv = rv['version'] || rv

    primary = client.cluster.next_primary
    @primary_address = primary.address
    @primary_description = primary.description
    @replica_set_name = client.cluster.topology.replica_set_name

    @topology ||= begin
      topology = client.cluster.topology.class.name.sub(/.*::/, '')
      topology = topology.gsub(/([A-Z])/) { |match| '_' + match.downcase }.sub(/^_/, '')
      if topology =~ /^replica_set/
        topology = 'replica_set'
      end
      topology.to_sym
    end

    @single_server = client.cluster.servers_list.length == 1
  end

  def basic_client
    # Do not cache the result here so that if the client gets closed,
    # client registry reconnects it in subsequent tests
    ClientRegistry.instance.global_client('basic')
  end
end
