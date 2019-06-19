require 'singleton'

class SpecConfig
  include Singleton

  def initialize
    @uri_options = {}
    if ENV['MONGODB_URI']
      @mongodb_uri = Mongo::URI.new(ENV['MONGODB_URI'])
      @uri_options = Mongo::Options::Mapper.transform_keys_to_symbols(@mongodb_uri.uri_options)
      if @uri_options[:replica_set]
        @addresses = @mongodb_uri.servers
        @connect_options = { connect: :replica_set, replica_set: @uri_options[:replica_set] }
      elsif @uri_options[:connect] == :sharded || ENV['TOPOLOGY'] == 'sharded_cluster'
        @addresses = @mongodb_uri.servers
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

    @uri_tls_options = {}
    @uri_options.each do |k, v|
      k = k.to_s.downcase
      if k.start_with?('ssl')
        @uri_tls_options[k] = v
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

  def retry_reads
    uri_option_or_env_var(:retry_reads, 'RETRY_READS')
  end

  def retry_writes
    uri_option_or_env_var(:retry_writes, 'RETRY_WRITES')
  end

  def uri_option_or_env_var(driver_option_symbol, env_var_key)
    case uri_options[driver_option_symbol]
    when true
      true
    when false
      false
    else
      case (ENV[env_var_key] || '').downcase
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

  def any_port
    addresses.first.split(':')[1] || '27017'
  end

  def spec_root
    File.join(File.dirname(__FILE__), '..')
  end

  def ssl_certs_dir
    Pathname.new("#{spec_root}/support/certificates")
  end

  # TLS certificates & keys

  def local_client_key_path
    "#{ssl_certs_dir}/client.key"
  end

  def client_key_path
    if drivers_tools? && ENV['DRIVER_TOOLS_CLIENT_KEY_PEM']
      ENV['DRIVER_TOOLS_CLIENT_KEY_PEM']
    else
      local_client_key_path
    end
  end

  def local_client_cert_path
    "#{ssl_certs_dir}/client.crt"
  end

  def client_cert_path
    if drivers_tools? && ENV['DRIVER_TOOLS_CLIENT_CERT_PEM']
      ENV['DRIVER_TOOLS_CLIENT_CERT_PEM']
    else
      local_client_cert_path
    end
  end

  def local_client_pem_path
    "#{ssl_certs_dir}/client.pem"
  end

  def client_pem_path
    if drivers_tools? && ENV['DRIVER_TOOLS_CLIENT_CERT_KEY_PEM']
      ENV['DRIVER_TOOLS_CLIENT_CERT_KEY_PEM']
    else
      local_client_pem_path
    end
  end

  def second_level_cert_path
    "#{ssl_certs_dir}/client-second-level.crt"
  end

  def second_level_key_path
    "#{ssl_certs_dir}/client-second-level.key"
  end

  def second_level_cert_bundle_path
    "#{ssl_certs_dir}/client-second-level-bundle.pem"
  end

  def local_client_encrypted_key_path
    "#{ssl_certs_dir}/client-encrypted.key"
  end

  def client_encrypted_key_path
    if drivers_tools? && ENV['DRIVER_TOOLS_CLIENT_KEY_ENCRYPTED_PEM']
      ENV['DRIVER_TOOLS_CLIENT_KEY_ENCRYPTED_PEM']
    else
      local_client_encrypted_key_path
    end
  end

  def client_encrypted_key_passphrase
    'passphrase'
  end

  def local_ca_cert_path
    "#{ssl_certs_dir}/ca.crt"
  end

  def ca_cert_path
    if drivers_tools? && ENV['DRIVER_TOOLS_CA_PEM']
      ENV['DRIVER_TOOLS_CA_PEM']
    else
      local_ca_cert_path
    end
  end

  def multi_ca_path
    "#{ssl_certs_dir}/multi-ca.crt"
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
        ssl_verify: true,
        ssl_cert:  client_cert_path,
        ssl_key:  client_key_path,
        ssl_ca_cert: ca_cert_path,
      }.merge(@uri_tls_options)
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
