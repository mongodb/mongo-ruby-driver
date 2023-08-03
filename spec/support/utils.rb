# frozen_string_literal: true

autoload :Base64, 'base64'
autoload :JSON, 'json'
module Net
  autoload :HTTP, 'net/http'
end

module Utils
  extend self

  # Used by #yamlify_command_events
  MAP_REDUCE_COMMANDS = %w[ map reduce ].freeze

  # Used by #yamlify_command_events
  AUTHENTICATION_COMMANDS = %w[ saslStart saslContinue authenticate getnonce ].freeze

  # The system command to invoke to represent a false result
  BIN_FALSE = File.executable?('/bin/false') ? '/bin/false' : 'false'

  # The system command to invoke to represent a true result
  BIN_TRUE  = File.executable?('/bin/true') ? '/bin/true' : 'true'

  # Converts a 'camelCase' string or symbol to a :under_score symbol.
  def underscore(str)
    str = str.to_s
    str = str[0].downcase + str[1...str.length].gsub(/([A-Z]+)/) { |m| "_#{m.downcase}" }
    str.to_sym
  end

  # Creates a copy of a hash where all keys and string values are converted to
  # snake-case symbols.
  #
  # For example, { 'fooBar' => { 'baz' => 'bingBing', :x => 1 } } converts to
  # { :foo_bar => { :baz => :bing_bing, :x => 1 } }.
  def underscore_hash(value)
    return value unless value.is_a?(Hash)

    value.reduce({}) do |hash, (k, v)|
      hash.tap do |h|
        h[underscore(k)] = underscore_hash(v)
      end
    end
  end

  # Creates a copy of a hash where all keys and string values are converted to
  # snake-case symbols.
  #
  # For example, { 'fooBar' => { 'baz' => 'bingBing', :x => 1 } } converts to
  # { :foo_bar => { :baz => :bing_bing, :x => 1 } }.
  def shallow_underscore_hash(value)
    return value unless value.is_a?(Hash)

    value.reduce({}) do |hash, (k, v)|
      hash.tap do |h|
        h[underscore(k)] = v
      end
    end
  end

  # Creates a copy of a hash where all keys and string values are converted to
  # snake-case symbols.
  #
  # For example, { 'fooBar' => { 'baz' => 'bingBing', :x => 1 } } converts to
  # { :foo_bar => { :baz => :bing_bing, :x => 1 } }.
  def snakeize_hash(value)
    return underscore(value) if value.is_a?(String)

    case value
    when Array
      value.map do |sub|
        case sub
        when Hash
          snakeize_hash(sub)
        else
          sub
        end
      end
    when Hash
      value.reduce({}) do |hash, (k, v)|
        hash.tap do |h|
          h[underscore(k)] = snakeize_hash(v)
        end
      end
    else
      value
    end
  end

  # Like snakeize_hash but does not recurse.
  def shallow_snakeize_hash(value)
    return underscore(value) if value.is_a?(String)
    return value unless value.is_a?(Hash)

    value.reduce({}) do |hash, (k, v)|
      hash.tap do |h|
        h[underscore(k)] = v
      end
    end
  end

  # Creates a copy of a hash where all keys and symbol values are converted to
  # camel-case strings.
  #
  # For example, { :foo_bar => { :baz => :bing_bing, 'x' => 1 } } converts to
  # { 'fooBar' => { 'baz' => 'bingBing', 'x' => 1 } }.
  def camelize_hash(value, upcase_first = false)
    return camelize(value.to_s, upcase_first) if value.is_a?(Symbol)
    return value unless value.is_a?(Hash)

    value.reduce({}) do |hash, (k, v)|
      hash.tap do |h|
        h[camelize(k.to_s)] = camelize_hash(v, upcase_first)
      end
    end
  end

  def camelize(str, upcase_first = false)
    str = str.gsub(/_(\w)/) { |m| m[1].upcase }
    str = str[0].upcase + str[1...str.length] if upcase_first
    str
  end

  def downcase_keys(hash)
    hash.transform_keys(&:downcase)
  end

  def disable_retries_client_options
    {
      retry_reads: false,
      retry_writes: false,
      max_read_retries: 0,
      max_write_retries: 0,
    }
  end

  # Converts camel case clientOptions, as used in spec tests,
  # to Ruby driver underscore options.
  def convert_client_options(spec_test_options)
    mapper = Mongo::URI::OptionsMapper.new
    spec_test_options.each_with_object({}) do |(name, value), opts|
      if name == 'autoEncryptOpts'
        auto_encryption_options = convert_auto_encryption_client_options(value)
        opts[:auto_encryption_options] = auto_encryption_options
      else
        mapper.add_uri_option(name, value.to_s, opts)
      end

      opts
    end
  end

  def order_hash(hash)
    hash.to_a.sort.to_h
  end

  # Transforms an array of CommandStarted events to an array of hashes
  # matching event specification in YAML spec files
  # rubocop:disable Metrics, Style/IfUnlessModifier
  def yamlify_command_events(events)
    events = events.map do |e|
      command = e.command.dup

      # Fake BSON::Code for map/reduce commands
      MAP_REDUCE_COMMANDS.each do |key|
        command[key] = BSON::Code.new(command[key]) if command[key].is_a?(String)
      end

      if command['readConcern']
        # The spec test use an afterClusterTime value of 42 to indicate that we need to assert
        # that the field exists in the actual read concern rather than comparing the value, so
        # we replace any afterClusterTime value with 42.
        if command['readConcern']['afterClusterTime']
          command['readConcern']['afterClusterTime'] = 42
        end

        # Convert the readConcern level from a symbol to a string.
        if command['readConcern']['level']
          command['readConcern']['level'] = command['readConcern']['level'].to_s
        end
      end

      if command['recoveryToken']
        command['recoveryToken'] = 42
      end

      # The spec tests use 42 as a placeholder value for any getMore cursorId.
      command['getMore'] = command['getMore'].class.new(42) if command['getMore']

      # Remove fields if empty
      command.delete('query') if command['query'] && command['query'].empty?

      {
        'command_started_event' => order_hash(
          'command' => order_hash(command),
          'command_name' => e.command_name.to_s,
          'database_name' => e.database_name
        )
      }
    end

    # Remove any events from authentication commands.
    events.reject! do |e|
      command_name = e['command_started_event']['command_name']
      AUTHENTICATION_COMMANDS.include?(command_name)
    end

    events
  end
  # rubocop:enable Metrics, Style/IfUnlessModifier

  # rubocop:disable Metrics
  def convert_operation_options(options)
    if options
      options.map do |k, v|
        out_v =
          case k
          when 'readPreference'
            out_k = :read
            out_v = {}
            v.each do |sub_k, sub_v|
              if sub_k == 'mode'
                out_v[:mode] = Utils.underscore(v['mode'])
              else
                out_v[sub_k.to_sym] = sub_v
              end
            end
            out_v
          when 'defaultTransactionOptions'
            out_k = Utils.underscore(k).to_sym
            convert_operation_options(v)
          when 'readConcern'
            out_k = Utils.underscore(k).to_sym
            Mongo::Options::Mapper.transform_keys_to_symbols(v).tap do |out|
              out[:level] = out[:level].to_sym if out[:level]
            end
          when 'causalConsistency'
            out_k = Utils.underscore(k).to_sym
            v
          when 'writeConcern'
            # Tests added in SPEC-1352 specify {writeConcern: {}} but what
            # they mean is for the driver to use the default write concern,
            # which for Ruby means no write concern is specified at all.
            #
            # This nil return requires the compact call below to get rid of
            # the nils before outgoing options are constructed.
            next nil if v == {}

            # Write concern option is called :write on the client, but
            # :write_concern on all levels below the client.
            out_k = :write_concern
            # The client expects write concern value to only have symbol keys.
            v.transform_keys(&:to_sym)
          else
            raise "Unhandled operation option #{k}"
          end
        [ out_k, out_v ]
      end.compact.to_h
    else
      {}
    end
  end
  # rubocop:enable Metrics

  def int64_value(value)
    if value.respond_to?(:value)
      # bson-ruby >= 4.6.0
      value.value
    else
      value.instance_variable_get(:@integer)
    end
  end

  URI_OPTION_MAP = {
    app_name: 'appName',
    auth_mech: 'authMechanism',
    auth_source: 'authsource',
    replica_set: 'replicaSet',
    ssl_ca_cert: 'tlsCAFile',
    ssl_cert: 'tlsCertificateKeyFile',
    ssl_key: 'tlsCertificateKeyFile',
  }.freeze

  # rubocop:disable Metrics
  def create_mongodb_uri(address_strs, **opts)
    creds = opts[:username] ? "#{opts[:username]}:#{opts[:password]}@" : ''

    uri = +"mongodb://#{creds}#{address_strs.join(',')}/"
    uri << opts[:database] if opts[:database]

    if (uri_options = opts[:uri_options])
      uri << '?'

      uri_options.each do |k, v|
        uri << '&'

        write_k = URI_OPTION_MAP[k] || k

        case k
        when :compressors
          write_v = v.join(',')
        when :auth_mech
          next unless v

          write_v = Mongo::URI::AUTH_MECH_MAP.key(v)
          raise "Unhandled auth mech value: #{v}" unless write_v
        else
          write_v = v
        end

        uri << "#{write_k}=#{write_v}"
      end
    end

    uri
  end
  # rubocop:enable Metrics

  # Client-Side encryption tests introduce the $$type syntax for determining
  # equality in command started events. The $$type key specifies which type of
  # BSON object is expected in the result. If the $$type key is present, only
  # check the class of the result.
  # rubocop:disable Metrics
  def match_with_type?(expected, actual)
    if expected.is_a?(Hash) && expected.key?('$$type')
      case expected['$$type']
      when 'binData'
        expected_class = BSON::Binary
        expected_key = '$binary'
      when 'long'
        expected_class = BSON::Int64
        expected_key = '$numberLong'
      else
        raise "Tests do not currently support matching against $$type #{v['$$type']}"
      end

      actual.is_a?(expected_class) || actual.key?(expected_key)
    elsif expected.is_a?(Hash) && actual.is_a?(Hash)
      has_all_keys = (expected.keys - actual.keys).empty?

      same_values = expected.keys.all? do |key|
        match_with_type?(expected[key], actual[key])
      end

      has_all_keys && same_values
    elsif expected.is_a?(Array) && actual.is_a?(Array)
      same_length = expected.length == actual.length

      same_values = expected.map.with_index do |_, idx|
        match_with_type?(expected[idx], actual[idx])
      end.all?

      same_length && same_values
    elsif expected == 42
      actual.is_a?(Numeric) || actual.is_a?(BSON::Int32) || actual.is_a?(BSON::Int64)
    else
      expected == actual
    end
  end
  # rubocop:enable Metrics

  # Takes a timeout and a block. Waits up to the specified timeout until
  # the value of the block is true. If timeout is reached, this method
  # returns normally and does not raise an exception. The block is invoked
  # every second or so.
  def wait_for_condition(timeout)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      break if yield ||
               Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 1
    end
  end

  def ensure_port_free(port)
    TCPServer.open(port) do
      # Nothing
    end
  end

  def wait_for_port_free(port, timeout)
    wait_for_condition(timeout) do
      ensure_port_free(port)
      true
    rescue Errno::EADDRINUSE
      false
    end
  end

  def get_ec2_metadata_token(ttl: 30, http: nil)
    http ||= Net::HTTP.new('169.254.169.254')
    # The TTL is required in order to obtain the metadata token.
    req = Net::HTTP::Put.new('/latest/api/token',
                             { 'x-aws-ec2-metadata-token-ttl-seconds' => ttl.to_s })
    resp = http.request(req)
    raise "Metadata token request failed: #{e.class}: #{e}" if resp.code != '200'

    resp.body
  end

  def ec2_instance_id
    http = Net::HTTP.new('169.254.169.254')
    metadata_token = get_ec2_metadata_token(http: http)
    req = Net::HTTP::Get.new('/latest/dynamic/instance-identity/document',
                             { 'x-aws-ec2-metadata-token' => metadata_token })
    resp = http.request(req)
    payload = JSON.parse(resp.body)
    payload.fetch('instanceId')
  end

  def ec2_instance_profile
    http = Net::HTTP.new('169.254.169.254')
    metadata_token = get_ec2_metadata_token(http: http)
    req = Net::HTTP::Get.new('/latest/meta-data/iam/info',
                             { 'x-aws-ec2-metadata-token' => metadata_token })
    resp = http.request(req)
    return nil if resp.code == '404'

    payload = JSON.parse(resp.body)
    payload['InstanceProfileArn']
  end

  def wait_for_instance_profile
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
    loop do
      begin
        ip = ec2_instance_profile
        if ip
          puts "Instance profile assigned: #{ip}"
          break
        end
      rescue StandardError => e
        puts "Problem retrieving instance profile: #{e.class}: #{e}"
      end

      if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        raise 'Instance profile did not get assigned in 15 seconds'
      end

      sleep 3
    end
  end

  def wait_for_no_instance_profile
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
    loop do
      begin
        ip = ec2_instance_profile
        if ip.nil?
          puts 'Instance profile cleared'
          break
        end
      rescue StandardError => e
        puts "Problem retrieving instance profile: #{e.class}: #{e}"
      end

      if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        raise 'Instance profile did not get cleared in 15 seconds'
      end

      sleep 3
    end
  end

  def wrap_forked_child
    yield
  rescue StandardError => e
    warn "Failing process #{Process.pid} due to #{e.class}: #{e}"
    exec(BIN_FALSE)
  else
    # Exec so that we do not close any clients etc. in the child.
    exec(BIN_TRUE)
  end

  def subscribe_all(client, subscriber)
    subscribe_all_sdam_proc(subscriber).call(client)
  end

  def subscribe_all_sdam_proc(subscriber)
    lambda do |client|
      client.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING, subscriber)
      client.subscribe(Mongo::Monitoring::SERVER_OPENING, subscriber)
      client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, subscriber)
      client.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED, subscriber)
      client.subscribe(Mongo::Monitoring::SERVER_CLOSED, subscriber)
      client.subscribe(Mongo::Monitoring::TOPOLOGY_CLOSED, subscriber)

      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)

      client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)

      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  # Creates an event subscriber, subscribes it to command events on the
  # specified client, invokes the passed block, asserts there is exactly one
  # command event published, asserts the command event published has the
  # specified command name, and returns the published event.
  def get_command_event(client, command_name, include_auth: false)
    subscriber = Mrss::EventSubscriber.new
    client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    begin
      yield client
    ensure
      client.unsubscribe(Mongo::Monitoring::COMMAND, subscriber)
    end

    subscriber.single_command_started_event(command_name, include_auth: include_auth)
  end

  # Drops and creates a collection for the purpose of starting the test from
  # a clean slate.
  #
  # @param [ Mongo::Client ] client
  # @param [ String ] collection_name
  def create_collection(client, collection_name)
    client[collection_name].drop
    client[collection_name].create
  end

  # If the deployment is a sharded cluster, creates a direct client
  # to each of the mongos nodes and yields each in turn to the
  # provided block. Does nothing in other topologies.
  # rubocop:disable Metrics
  def mongos_each_direct_client
    return unless ClusterConfig.instance.topology == :sharded

    client = ClientRegistry.instance.global_client('basic')
    client.cluster.next_primary
    client.cluster.servers.each do |server|
      direct_client = ClientRegistry.instance.new_local_client(
        [ server.address.to_s ],
        SpecConfig.instance.test_options.merge(
          connect: :sharded
        ).merge(SpecConfig.instance.auth_options)
      )
      yield direct_client
      direct_client.close
    end
  end
  # rubocop:enable Metrics

  # rubocop:disable Metrics
  def permitted_yaml_classes
    @permitted_yaml_classes ||= [
      BigDecimal,
      Date,
      Time,
      Range,
      Regexp,
      Symbol,
      BSON::Binary,
      BSON::Code,
      BSON::CodeWithScope,
      BSON::DbPointer,
      BSON::Decimal128,
      BSON::Int32,
      BSON::Int64,
      BSON::MaxKey,
      BSON::MinKey,
      BSON::ObjectId,
      BSON::Regexp::Raw,
      BSON::Symbol::Raw,
      BSON::Timestamp,
      BSON::Undefined,
    ].freeze
  end
  # rubocop:enable Metrics

  def load_spec_yaml_file(path)
    if RUBY_VERSION < '2.6'
      YAML.safe_load(File.read(path), permitted_yaml_classes, [], true)
    else
      # Here we have Ruby 2.6+ that supports the new syntax of `safe_load``.
      YAML.safe_load(File.read(path), permitted_classes: permitted_yaml_classes, aliases: true)
    end
  end

  private

  def convert_auto_encryption_client_options(opts)
    auto_encrypt_opts = Utils.snakeize_hash(opts)

    _apply_kms_providers(opts, auto_encrypt_opts)

    _apply_key_vault_namespace(opts, auto_encrypt_opts)
    _apply_schema_map(opts, auto_encrypt_opts)
    _apply_encrypted_fields_map(opts, auto_encrypt_opts)

    auto_encrypt_opts.merge!(extra_options: convert_auto_encryption_extra_options(auto_encrypt_opts))
  end

  def _apply_kms_provider_aws(opts, auto_encrypt_opts)
    return unless opts['kmsProviders']['aws']

    # The tests require that AWS credentials be filled in by the driver.
    auto_encrypt_opts[:kms_providers][:aws] = {
      access_key_id: SpecConfig.instance.fle_aws_key,
      secret_access_key: SpecConfig.instance.fle_aws_secret,
    }
  end

  def _apply_kms_providers(opts, auto_encrypt_opts)
    _apply_kms_provider_aws(opts, auto_encrypt_opts)
    _apply_kms_provider_azure(opts, auto_encrypt_opts)
    _apply_kms_provider_gcp(opts, auto_encrypt_opts)
    _apply_kms_provider_local(opts, auto_encrypt_opts)
  end

  def _apply_kms_provider_azure(opts, auto_encrypt_opts)
    return unless opts['kmsProviders']['azure']

    # The tests require that Azure credentials be filled in by the driver.
    auto_encrypt_opts[:kms_providers][:azure] = {
      tenant_id: SpecConfig.instance.fle_azure_tenant_id,
      client_id: SpecConfig.instance.fle_azure_client_id,
      client_secret: SpecConfig.instance.fle_azure_client_secret,
    }
  end

  def _apply_kms_provider_gcp(opts, auto_encrypt_opts)
    return unless opts['kmsProviders']['gcp']

    # The tests require that GCP credentials be filled in by the driver.
    auto_encrypt_opts[:kms_providers][:gcp] = {
      email: SpecConfig.instance.fle_gcp_email,
      private_key: SpecConfig.instance.fle_gcp_private_key,
    }
  end

  def _apply_kms_provider_local(opts, auto_encrypt_opts)
    return unless opts['kmsProviders']['local']

    auto_encrypt_opts[:kms_providers][:local] = {
      key: BSON::ExtJSON.parse_obj(opts['kmsProviders']['local']['key']).data
    }
  end

  def _apply_key_vault_namespace(opts, auto_encrypt_opts)
    auto_encrypt_opts[:key_vault_namespace] =
      opts['keyVaultNamespace'] || 'keyvault.datakeys'
  end

  def _apply_schema_map(opts, auto_encrypt_opts)
    return unless opts['schemaMap']

    auto_encrypt_opts[:schema_map] = BSON::ExtJSON.parse_obj(opts['schemaMap'])
  end

  def _apply_encrypted_fields_map(opts, auto_encrypt_opts)
    return unless opts['encryptedFieldsMap']

    auto_encrypt_opts[:encrypted_fields_map] = BSON::ExtJSON.parse_obj(opts['encryptedFieldsMap'])
  end

  # rubocop:disable Metrics
  def convert_auto_encryption_extra_options(opts)
    # Spawn mongocryptd on non-default port for sharded cluster tests
    extra_options = {
      mongocryptd_spawn_args: [ "--port=#{SpecConfig.instance.mongocryptd_port}" ],
      mongocryptd_uri: "mongodb://localhost:#{SpecConfig.instance.mongocryptd_port}"
    }.merge(opts[:extra_options] || {})

    # if bypass_query_analysis has been explicitly specified, then we ignore
    # any requirement to use the shared library, as the two are not
    # compatible.
    if SpecConfig.instance.crypt_shared_lib_required && !opts[:bypass_query_analysis]
      extra_options[:crypt_shared_lib_required] = SpecConfig.instance.crypt_shared_lib_required
      extra_options[:crypt_shared_lib_path] = SpecConfig.instance.crypt_shared_lib_path
      extra_options[:mongocryptd_uri] = 'mongodb://localhost:27777'
    end

    extra_options
  end
  # rubocop:enable Metrics
end
