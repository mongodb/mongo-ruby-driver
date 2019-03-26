require 'singleton'

class SpecConfig
  include Singleton

  def initialize
    if ENV['MONGODB_URI']
      @mongodb_uri = Mongo::URI.new(ENV['MONGODB_URI'])
      @uri_options = Mongo::Options::Mapper.transform_keys_to_symbols(@mongodb_uri.uri_options)
      if @uri_options[:replica_set]
        @addresses = @mongodb_uri.servers
        @connect_options = { connect: :replica_set, replica_set: @uri_options[:replica_set] }
      elsif @uri_options[:connect] == :sharded || ENV['TOPOLOGY'] == 'sharded_cluster'
        # See SERVER-16836 for why we can only use one host:port
        if @mongodb_uri.servers.length > 1
          warn "Using only the first mongos (#{@mongodb_uri.servers.first})"
        end
        @addresses = [ @mongodb_uri.servers.first ]
        @connect_options = { connect: :sharded }
      elsif @uri_options[:connect] == :direct
        @addresses = @mongodb_uri.servers
        @connect_options = { connect: :direct }
      end
      if @uri_options[:ssl].nil?
        @ssl = (ENV['SSL'] == 'ssl') || (ENV['SSL_ENABLED'] == 'true')
      else
        @ssl = @uri_options[:ssl]
      end
    elsif ENV['MONGODB_ADDRESSES']
      @addresses = ENV['MONGODB_ADDRESSES'] ? ENV['MONGODB_ADDRESSES'].split(',').freeze : [ '127.0.0.1:27017' ].freeze
      if ENV['RS_ENABLED']
        @connect_options = { connect: :replica_set, replica_set: ENV['RS_NAME'] }
      elsif ENV['SHARDED_ENABLED']
        @connect_options = { connect: :sharded }
      else
        @connect_options = { connect: :direct }
      end
    end

    if @addresses.nil?
      # Discover deployment topology
      if @mongodb_uri
        # TLS options need to be merged for evergreen due to
        # https://github.com/10gen/mongo-orchestration/issues/268
        client = Mongo::Client.new(@mongodb_uri.servers, Mongo::Options::Redacted.new(
          server_selection_timeout: 5,
        ).merge(@mongodb_uri.uri_options).merge(ssl_options))
        @addresses = @mongodb_uri.servers
      else
        client = Mongo::Client.new(['localhost:27017'], server_selection_timeout: 5)
        @addresses = client.cluster.servers_list.map do |server|
          server.address.to_s
        end
      end
      client.cluster.next_primary
      case client.cluster.topology.class.name
      when /Replica/
        @connect_options = { connect: :replica_set, replica_set: client.cluster.topology.replica_set_name }
      when /Sharded/
        @connect_options = { connect: :sharded }
      when /Single/
        @connect_options = { connect: :direct }
      when /Unknown/
        raise "Could not detect topology because the test client failed to connect to MongoDB deployment"
      else
        raise "Weird topology #{client.cluster.topology}"
      end
    end
  end

  attr_reader :uri_options, :addresses, :connect_options

  # Environment

  def ci?
    !!ENV['CI']
  end

  def mri?
    !jruby?
  end

  def jruby?
    !!(RUBY_PLATFORM =~ /\bjava\b/)
  end

  def platform
    RUBY_PLATFORM
  end

  # Test suite configuration

  def client_debug?
    %w(1 true yes).include?((ENV['CLIENT_DEBUG'] || '').downcase)
  end

  def drivers_tools?
    !!ENV['DRIVERS_TOOLS']
  end

  def active_support?
    %w(1 true yes).include?(ENV['WITH_ACTIVE_SUPPORT'])
  end

  # What compressor to use, if any.
  def compressors
    if ENV['COMPRESSORS']
      ENV['COMPRESSORS'].split(',')
    else
      nil
    end
  end

  def retry_writes
    case uri_options[:retry_writes]
    when true
      true
    when false
      false
    else
      case (ENV['RETRY_WRITES'] || '').downcase
      when 'yes', 'true', 'on', '1'
        true
      when 'no', 'false', 'off', '0'
        false
      else
        nil
      end
    end
  end

  def retry_writes?
    if retry_writes == false
      false
    else
      # Current default is to retry writes
      true
    end
  end

  def ssl?
    @ssl
  end

  # Username, not user object
  def user
    @mongodb_uri && @mongodb_uri.credentials[:user]
  end

  def password
    @mongodb_uri && @mongodb_uri.credentials[:password]
  end

  def auth_source
    @uri_options && uri_options[:auth_source]
  end

  def connect_replica_set?
    connect_options[:connect] == :replica_set
  end

  def print_summary
    puts "Connection options: #{test_options}"
    client = ClientRegistry.instance.global_client('basic')
    client.cluster.next_primary
    puts <<-EOT
Topology: #{client.cluster.topology.class}
connect: #{connect_options[:connect]}
EOT
  end

  # Derived data

  # The write concern to use in the tests.
  def write_concern
    if connect_replica_set?
      {w: 2}
    else
      {w: 1}
    end
  end

  def any_port
    addresses.first.split(':')[1] || '27017'
  end

  def spec_root
    File.join(File.dirname(__FILE__), '..')
  end

  def ssl_certs_dir
    "#{spec_root}/support/certificates"
  end

  def client_cert_pem
    if drivers_tools?
      ENV['DRIVER_TOOLS_CLIENT_CERT_PEM']
    else
      "#{ssl_certs_dir}/client_cert.pem"
    end
  end

  def client_key_pem
    if drivers_tools?
      ENV['DRIVER_TOOLS_CLIENT_KEY_PEM']
    else
      "#{ssl_certs_dir}/client_key.pem"
    end
  end

  def client_cert_key_pem
    if drivers_tools?
      ENV['DRIVER_TOOLS_CLIENT_CERT_KEY_PEM']
    else
      "#{ssl_certs_dir}/client.pem"
    end
  end

  # The default test database for all specs.
  def test_db
    'ruby-driver'.freeze
  end

  # Option hashes

  def auth_options
    {
      user: user,
      password: password,
      auth_source: auth_source,
    }
  end

  def ssl_options
    if ssl?
      {
        ssl: true,
        ssl_verify: false,
        ssl_cert:  client_cert_pem,
        ssl_key:  client_key_pem,
      }
    else
      {}
    end
  end

  def compressor_options
    if compressors
      {compressors: compressors}
    else
      {}
    end
  end

  def retry_writes_options
    {retry_writes: retry_writes}
  end

  # Base test options.
  def base_test_options
    {
      max_pool_size: 1,
      write: write_concern,
      heartbeat_frequency: 20,
      max_read_retries: 5,
      # The test suite seems to perform a number of operations
      # requiring server selection. Hence a timeout of 1 here,
      # together with e.g. a misconfigured replica set,
      # means the test suite hangs for about 4 seconds before
      # failing.
      # Server selection timeout of 1 is insufficient for evergreen.
      server_selection_timeout: ssl? ? 4.01 : 2.01,
      wait_queue_timeout: 2,
      connect_timeout: 3,
      max_idle_time: 5
   }
  end

  # Options for test suite clients.
  def test_options
    base_test_options.merge(connect_options).
      merge(ssl_options).merge(compressor_options).merge(retry_writes_options)
  end

  # User objects

  # Gets the root system administrator user.
  def root_user
    Mongo::Auth::User.new(
      user: user || 'root-user',
      password: password || 'password',
      roles: [
        Mongo::Auth::Roles::USER_ADMIN_ANY_DATABASE,
        Mongo::Auth::Roles::DATABASE_ADMIN_ANY_DATABASE,
        Mongo::Auth::Roles::READ_WRITE_ANY_DATABASE,
        Mongo::Auth::Roles::HOST_MANAGER,
        Mongo::Auth::Roles::CLUSTER_ADMIN
      ]
    )
  end

  # Get the default test user for the suite on versions 2.6 and higher.
  def test_user
    Mongo::Auth::User.new(
      database: test_db,
      user: 'test-user',
      password: 'password',
      roles: [
        { role: Mongo::Auth::Roles::READ_WRITE, db: test_db },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: test_db },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'invalid_database' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'invalid_database' },

        # For transactions examples
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'hr' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'hr' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'reporting' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'reporting' },

        # For transaction api spec tests
        #{ role: Mongo::Auth::Roles::READ_WRITE, db: 'withTransaction-tests' },
        #{ role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'withTransaction-tests' },

      ]
    )
  end
end
