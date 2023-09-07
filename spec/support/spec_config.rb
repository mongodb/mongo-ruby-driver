# frozen_string_literal: true
# rubocop:todo all

require 'singleton'
require 'pathname'

class SpecConfig
  include Singleton

  # NB: constructor should not do I/O as SpecConfig may be used by tests
  # only loading the lite spec helper. Do I/O eagerly in accessor methods.
  def initialize
    @uri_options = {}
    @ruby_options = {}
    if ENV['MONGODB_URI']
      @mongodb_uri = Mongo::URI.get(ENV['MONGODB_URI'])
      @uri_options = Mongo::Options::Mapper.transform_keys_to_symbols(@mongodb_uri.uri_options)
      if ENV['TOPOLOGY'] == 'load-balanced'
        @addresses = @mongodb_uri.servers
        @connect_options = { connect: :load_balanced }
      elsif @uri_options[:replica_set]
        @addresses = @mongodb_uri.servers
        @connect_options = { connect: :replica_set, replica_set: @uri_options[:replica_set] }
      elsif @uri_options[:connect] == :sharded || ENV['TOPOLOGY'] == 'sharded-cluster'
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
    end

    @uri_tls_options = {}
    @uri_options.each do |k, v|
      k = k.to_s.downcase
      if k.start_with?('ssl')
        @uri_tls_options[k] = v
      end
    end

    @ssl ||= false

    if (server_api = ENV['SERVER_API']) && !server_api.empty?
      @ruby_options[:server_api] = BSON::Document.new(YAML.load(server_api))
      # Since the tests pass options provided by SpecConfig directly to
      # internal driver objects (e.g. connections), transform server api
      # parameters here as they would be transformed by Client constructor.
      if (v = @ruby_options[:server_api][:version]).is_a?(Integer)
        @ruby_options[:server_api][:version] = v.to_s
      end
    end
  end

  attr_reader :uri_options, :ruby_options, :connect_options

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
        server_selection_timeout: 5.03,
      ).merge(ssl_options).merge(ruby_options))

      begin
        case client.cluster.topology.class.name
        when /LoadBalanced/
          { connect: :load_balanced }
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
    %w(1 true yes).include?(ENV['CI']&.downcase)
  end

  def mri?
    !jruby?
  end

  def jruby?
    !!(RUBY_PLATFORM =~ /\bjava\b/)
  end

  def linux?
    !!(RbConfig::CONFIG['host_os'].downcase =~ /\blinux/)
  end

  def macos?
    !!(RbConfig::CONFIG['host_os'].downcase =~ /\bdarwin/)
  end

  def windows?
    ENV['OS'] == 'Windows_NT' && !RUBY_PLATFORM.match?(/cygwin/)
  end

  def platform
    RUBY_PLATFORM
  end

  def stress?
    %w(1 true yes).include?(ENV['STRESS']&.downcase)
  end

  def fork?
    %w(1 true yes).include?(ENV['FORK']&.downcase)
  end

  # OCSP tests require python and various dependencies.
  # Assumes an OCSP responder is running on port 8100 (configured externally
  # to the test suite).
  def ocsp?
    %w(1 true yes).include?(ENV['OCSP']&.downcase)
  end

  # OCSP tests require python and various dependencies.
  # When testing OCSP verifier, there cannot be a responder running on
  # port 8100 or the tests will fail.
  def ocsp_verifier?
    %w(1 true yes).include?(ENV['OCSP_VERIFIER']&.downcase)
  end

  def ocsp_connectivity?
    ENV.key?('OCSP_CONNECTIVITY') && ENV['OCSP_CONNECTIVITY'] != ''
  end

  # Detect whether specs are running against Mongodb Atlas serverless instance.
  # This method does not do any magic, it just checks whether environment
  # variable SERVERLESS is set. This is a recommended way to inform spec runners
  # that they are running against a serverless instance
  #
  # @return [ true | false ] Whether specs are running against a serverless instance.
  def serverless?
    !!ENV['SERVERLESS']
  end

  def kill_all_server_sessions?
    !serverless? && # Serverless instances do not support killAllSessions command.
      ClusterConfig.instance.fcv_ish >= '3.6'
  end

  # Test suite configuration

  def client_debug?
    %w(1 true yes).include?(ENV['MONGO_RUBY_DRIVER_CLIENT_DEBUG']&.downcase)
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

  def ocsp_files_dir
    Pathname.new("#{spec_root}/../.mod/drivers-evergreen-tools/.evergreen/ocsp")
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
    if (algo = ENV['OCSP_ALGORITHM'])&.empty?
      "#{ssl_certs_dir}/client.pem"
    else
      Pathname.new("#{spec_root}/support/ocsp/#{algo}/server.pem")
    end
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

  # Whether FLE tests should be enabled
  def fle?
    %w(1 true yes helper).include?(ENV['FLE']&.downcase)
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

  # AWS temporary access key id (set by set-temp-creds.sh)
  def fle_aws_temp_key
    ENV['CSFLE_AWS_TEMP_ACCESS_KEY_ID']
  end

  # AWS temporary secret access key (set by set-temp-creds.sh)
  def fle_aws_temp_secret
    ENV['CSFLE_AWS_TEMP_SECRET_ACCESS_KEY']
  end

  # AWS temporary session token (set by set-temp-creds.sh)
  def fle_aws_temp_session_token
    ENV['CSFLE_AWS_TEMP_SESSION_TOKEN']
  end

  def fle_azure_tenant_id
    ENV['MONGO_RUBY_DRIVER_AZURE_TENANT_ID']
  end

  def fle_azure_client_id
    ENV['MONGO_RUBY_DRIVER_AZURE_CLIENT_ID']
  end

  def fle_azure_client_secret
    ENV['MONGO_RUBY_DRIVER_AZURE_CLIENT_SECRET']
  end

  def fle_azure_identity_platform_endpoint
    ENV['MONGO_RUBY_DRIVER_AZURE_IDENTITY_PLATFORM_ENDPOINT']
  end

  def fle_azure_key_vault_endpoint
    ENV['MONGO_RUBY_DRIVER_AZURE_KEY_VAULT_ENDPOINT']
  end

  def fle_azure_key_name
    ENV['MONGO_RUBY_DRIVER_AZURE_KEY_NAME']
  end

  def fle_gcp_email
    ENV['MONGO_RUBY_DRIVER_GCP_EMAIL']
  end

  def fle_gcp_private_key
    ENV['MONGO_RUBY_DRIVER_GCP_PRIVATE_KEY']
  end

  def fle_gcp_endpoint
    ENV['MONGO_RUBY_DRIVER_GCP_ENDPOINT']
  end

  def fle_gcp_project_id
    ENV['MONGO_RUBY_DRIVER_GCP_PROJECT_ID']
  end

  def fle_gcp_location
    ENV['MONGO_RUBY_DRIVER_GCP_LOCATION']
  end

  def fle_gcp_key_ring
    ENV['MONGO_RUBY_DRIVER_GCP_KEY_RING']
  end

  def fle_gcp_key_name
    ENV['MONGO_RUBY_DRIVER_GCP_KEY_NAME']
  end

  def fle_gcp_key_version
    ENV['MONGO_RUBY_DRIVER_GCP_KEY_VERSION']
  end

  def fle_kmip_endpoint
    "localhost:5698"
  end

  def fle_kmip_tls_ca_file
    "#{spec_root}/../.evergreen/x509gen/ca.pem"
  end

  def fle_kmip_tls_certificate_key_file
    "#{spec_root}/../.evergreen/x509gen/client.pem"
  end

  def mongocryptd_port
    if ENV['MONGO_RUBY_DRIVER_MONGOCRYPTD_PORT'] &&
      !ENV['MONGO_RUBY_DRIVER_MONGOCRYPTD_PORT'].empty?
    then
      ENV['MONGO_RUBY_DRIVER_MONGOCRYPTD_PORT'].to_i
    else
      27020
    end
  end

  def crypt_shared_lib_path
    if @without_crypt_shared_lib_path
      nil
    else
      ENV['MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH']
    end
  end

  def without_crypt_shared_lib_path
    saved, @without_crypt_shared_lib_path = @without_crypt_shared_lib_path, true
    yield
  ensure
    @without_crypt_shared_lib_path = saved
  end

  attr_accessor :crypt_shared_lib_required

  def require_crypt_shared
    saved, self.crypt_shared_lib_required = crypt_shared_lib_required, true
    yield
  ensure
    self.crypt_shared_lib_required = saved
  end

  def auth?
    x509_auth? || user
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
      }.tap do |options|
        if auth_source
          options[:auth_source] = auth_source
        end
        %i(auth_mech auth_mech_properties).each do |key|
          if uri_options[key]
            options[key] = uri_options[key]
          end
        end
      end
    end
  end

  def ssl_options
    return {} unless ssl?
    {
        ssl: true,
        ssl_verify: true,
    }.tap do |options|
      # We should use bundled cetificates for ssl except for testing against
      # Atlas instances. Atlas instances have addresses in domains
      # mongodb.net or mongodb-dev.net.
      if @mongodb_uri.servers.grep(/mongodb.*\.net/).empty?
        options.merge!(
          {
            ssl_cert: client_cert_path,
            ssl_key: client_key_path,
            ssl_ca_cert: ca_cert_path,
          }
        )
      end
    end.merge(Utils.underscore_hash(@uri_tls_options))
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

  # The options needed for a successful socket connection to the server(s).
  # These exclude options needed to handshake (e.g. server api parameters).
  def connection_options
    ssl_options
  end

  # The options needed for successful monitoring of the server(s).
  # These exclude options needed to perform operations (e.g. credentials).
  def monitoring_options
    ssl_options.merge(
      server_api: ruby_options[:server_api],
    )
  end

  # Base test options.
  def base_test_options
    {
      # Automatic encryption tests require a minimum of three connections:
      # - The driver checks out a connection to build a command.
      # - It may need to encrypt the command, which could require a query to
      #   the key vault collection triggered by libmongocrypt.
      # - If the key vault client has auto encryption options, it will also
      #   attempt to encrypt this query, resulting in a third connection.
      # In the worst case using FLE may end up tripling the number of
      # connections that the driver uses at any one time.
      max_pool_size: 3,

      heartbeat_frequency: 20,

      # The test suite seems to perform a number of operations
      # requiring server selection. Hence a timeout of 1 here,
      # together with e.g. a misconfigured replica set,
      # means the test suite hangs for about 4 seconds before
      # failing.
      # Server selection timeout of 1 is insufficient for evergreen.
      server_selection_timeout: uri_options[:server_selection_timeout] || (ssl? ? 8.01 : 7.01),

      # Since connections are established under the wait queue timeout,
      # the wait queue timeout should be at least as long as the
      # connect timeout.
      wait_queue_timeout: 6.04,
      connect_timeout: 2.91,
      socket_timeout: 5.09,
      max_idle_time: 100.02,

      # Uncomment to have exceptions in background threads log complete
      # backtraces.
      #bg_error_backtrace: true,
    }.merge(ruby_options).merge(
      server_api: ruby_options[:server_api] && ::Utils.underscore_hash(ruby_options[:server_api])
    )
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

  def authorized_test_options
    test_options.merge(credentials_or_external_user(
      user: test_user.name,
      password: test_user.password,
      auth_source: auth_options[:auth_source],
    ))
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
    # When testing against a serverless instance, we are not allowed to create
    # new users, we just have one user for everyhing.
    return root_user if serverless?

    Mongo::Auth::User.new(
      database: 'admin',
      user: 'ruby-test-user',
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

        # For spec tests
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'crud-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'crud-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'crud-default' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'crud-default' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'default_write_concern_db' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'default_write_concern_db' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'retryable-reads-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'retryable-reads-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'sdam-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'sdam-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'transaction-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'transaction-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'withTransaction-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'withTransaction-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'admin' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'admin' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'command-monitoring-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'command-monitoring-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'session-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'session-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'gridfs-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'gridfs-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'change-stream-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'change-stream-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'change-stream-tests-2' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'change-stream-tests-2' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'retryable-writes-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'retryable-writes-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'ts-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'ts-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'ci-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'ci-tests' },
        { role: Mongo::Auth::Roles::READ_WRITE, db: 'papi-tests' },
        { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: 'papi-tests' },
      ]
    )
  end

  def x509_auth?
    uri_options[:auth_mech] == :mongodb_x509
  end

  # When we authenticate with a username & password mechanism (scram, cr)
  # we create a variety of users in the test suite for different purposes.
  # When we authenticate with passwordless mechanisms (x509, aws) we use
  # the globally specified user for all operations.
  def external_user?
    case uri_options[:auth_mech]
    when :mongodb_x509, :aws
      true
    when nil, :scram, :scram256
      false
    else
      raise "Unknown auth mechanism value: #{uri_options[:auth_mech]}"
    end
  end

  # When we use external authentication, omit all of the users we normally
  # create and authenticate with the external mechanism. This also ensures
  # our various helpers work correctly when the only users available are
  # the external ones.
  def credentials_or_external_user(creds)
    if external_user?
      auth_options
    else
      creds
    end
  end

  # Returns whether the test suite was configured with a single mongos.
  def single_mongos?
    %w(1 true yes).include?(ENV['SINGLE_MONGOS'])
  end
end
