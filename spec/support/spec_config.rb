require 'singleton'
require 'pathname'

class SpecConfig
  include Singleton

  # NB: constructor should not do I/O as SpecConfig may be used by tests
  # only loading the lite spec helper. Do I/O eagerly in accessor methods.
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

    @ssl ||= false
  end

  attr_reader :uri_options, :connect_options

  def addresses
    @addresses ||= begin
      if @mongodb_uri
        @mongodb_uri.servers
      else
        client = Mongo::Client.new(['localhost:27017'], server_selection_timeout: 5.02)
        begin
          client.cluster.next_primary
          @addresses = client.cluster.servers_list.map do |server|
            server.address.to_s
          end
        ensure
          client.close
        end
      end
    end
  end

  def connect_options
    @connect_options ||= begin
      # Discover deployment topology.
      # TLS options need to be merged for evergreen due to
      # https://github.com/10gen/mongo-orchestration/issues/268
      client = Mongo::Client.new(addresses, Mongo::Options::Redacted.new(
        server_selection_timeout: 5,
      ).merge(ssl_options))

      begin
        case client.cluster.topology.class.name
        when /Replica/
          { connect: :replica_set, replica_set: client.cluster.topology.replica_set_name }
        when /Sharded/
          { connect: :sharded }
        when /Single/
          { connect: :direct }
        when /Unknown/
          raise "Could not detect topology because the test client failed to connect to MongoDB deployment"
        else
          raise "Weird topology #{client.cluster.topology}"
        end
      ensure
        client.close
      end
    end
  end

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

  def stress_spec?
    !!ENV['STRESS_SPEC']
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
    uri_options[:compressors]
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
    uri_options[:auth_source]
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

  def client_x509_pem_path
    "#{ssl_certs_dir}/client-x509.pem"
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

  # AWS IAM user access key id
  def fle_aws_key
    ENV['MONGO_RUBY_DRIVER_AWS_KEY']
  end

  # AWS IAM user secret access key
  def fle_aws_secret
    ENV['MONGO_RUBY_DRIVER_AWS_SECRET']
  end

  # Region of AWS customer master key
  def fle_aws_region
    ENV['MONGO_RUBY_DRIVER_AWS_REGION']
  end

  # Amazon resource name (ARN) of AWS customer master key
  def fle_aws_arn
    ENV['MONGO_RUBY_DRIVER_AWS_ARN']
  end

  # Option hashes

  def auth_options
    if x509_auth?
      {
        auth_mech: uri_options[:auth_mech],
        auth_source: '$external',
      }
    else
      {
        user: user,
        password: password,
        auth_source: auth_source,
      }
    end
  end

  def ssl_options
    if ssl?
      {
        ssl: true,
        ssl_verify: true,
        ssl_cert:  client_cert_path,
        ssl_key:  client_key_path,
        ssl_ca_cert: ca_cert_path,
      }.merge(Utils.underscore_hash(@uri_tls_options))
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
    {retry_writes: retry_writes?}
  end

  # Base test options.
  def base_test_options
    {
      # Automatic encryption tests require a minimum of two connections,
      # because the driver checks out a connection to build a command,
      # and then may need to encrypt the command which could require a
      # query to key vault connection triggered from libmongocrypt.
      # In the worst case using FLE may end up doubling the number of
      # connections that the driver uses at any one time.
      max_pool_size: 2,

      heartbeat_frequency: 20,

      # The test suite seems to perform a number of operations
      # requiring server selection. Hence a timeout of 1 here,
      # together with e.g. a misconfigured replica set,
      # means the test suite hangs for about 4 seconds before
      # failing.
      # Server selection timeout of 1 is insufficient for evergreen.
      server_selection_timeout: uri_options[:server_selection_timeout] || (ssl? ? 4.01 : 2.01),

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

  # TODO auth_options should probably be in test_options
  def all_test_options
    test_options.merge(auth_options)
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

  def x509_auth?
    uri_options[:auth_mech] == :mongodb_x509
  end

  # When we use x.509 authentication, omit all of the users we normally create
  # and authenticate with x.509.
  def credentials_or_x509(creds)
    if x509_auth?
      {auth_mech: :mongodb_x509}
    else
      creds
    end
  end
end
