autoload :Base64, 'base64'
autoload :JSON, 'json'
module Net
  autoload :HTTP, 'net/http'
end

module Utils
  # Converts a 'camelCase' string or symbol to a :under_score symbol.
  def underscore(str)
    str = str.to_s
    str = str[0].downcase + str[1...str.length].gsub(/([A-Z]+)/) { |m| "_#{m.downcase}" }
    str.to_sym
  end
  module_function :underscore

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
  module_function :underscore_hash

  # Creates a copy of a hash where all keys and string values are converted to
  # snake-case symbols.
  #
  # For example, { 'fooBar' => { 'baz' => 'bingBing', :x => 1 } } converts to
  # { :foo_bar => { :baz => :bing_bing, :x => 1 } }.
  def snakeize_hash(value)
    return underscore(value) if value.is_a?(String)
    return value unless value.is_a?(Hash)

    value.reduce({}) do |hash, (k, v)|
      hash.tap do |h|
        h[underscore(k)] = snakeize_hash(v)
      end
    end
  end
  module_function :snakeize_hash

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
  module_function :shallow_snakeize_hash

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
  module_function :camelize_hash

  def camelize(str, upcase_first = false)
    str = str.gsub(%r,_(\w),) { |m| m[1].upcase }
    if upcase_first
      str = str[0].upcase + str[1...str.length]
    end
    str
  end
  module_function :camelize

  module_function def disable_retries_client_options
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
    uri = Mongo::URI.new('mongodb://localhost')
    spec_test_options.reduce({}) do |opts, (name, value)|
      if name == 'autoEncryptOpts'
        opts.merge!(
          auto_encryption_options: convert_auto_encryption_client_options(value)
            .merge(
              # Spawn mongocryptd on non-default port for sharded cluster tests
              extra_options: {
                mongocryptd_spawn_args: ["--port=#{SpecConfig.instance.mongocryptd_port}"],
                mongocryptd_uri: "mongodb://localhost:#{SpecConfig.instance.mongocryptd_port}",
              }
            )
        )
      else
        uri.send(:add_uri_option, name, value.to_s, opts)
      end

      opts
    end
  end
  module_function :convert_client_options

  private def convert_auto_encryption_client_options(opts)
    auto_encrypt_opts = Utils.snakeize_hash(opts)

    if opts['kmsProviders']['aws']
      # The tests require that AWS credentials be filled in by the driver.
      auto_encrypt_opts[:kms_providers][:aws] = {
        access_key_id: SpecConfig.instance.fle_aws_key,
        secret_access_key: SpecConfig.instance.fle_aws_secret,
      }
    end

    if opts['kmsProviders']['local']
      auto_encrypt_opts[:kms_providers][:local] = {
        key: BSON::ExtJSON.parse_obj(opts['kmsProviders']['local']['key']).data
      }
    end

    if opts['keyVaultNamespace']
      auto_encrypt_opts[:key_vault_namespace] = opts['keyVaultNamespace']
    else
      auto_encrypt_opts[:key_vault_namespace] = 'admin.datakeys'
    end

    if opts['schemaMap']
      auto_encrypt_opts[:schema_map] = BSON::ExtJSON.parse_obj(opts['schemaMap'])
    end

    auto_encrypt_opts
  end
  module_function :convert_auto_encryption_client_options

  def order_hash(hash)
    Hash[hash.to_a.sort]
  end
  module_function :order_hash

  # Transforms an array of CommandStarted events to an array of hashes
  # matching event specification in YAML spec files
  def yamlify_command_events(events)
    events = events.map do |e|
      command = e.command.dup

      # Fake BSON::Code for map/reduce commands
      %w(map reduce).each do |key|
        if command[key].is_a?(String)
          command[key] = BSON::Code.new(command[key])
        end
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
      #command.delete('filter') if command['filter'] && command['filter'].empty?
      command.delete('query') if command['query'] && command['query'].empty?

      {
        'command_started_event' => order_hash(
          'command' => order_hash(command),
          'command_name' => e.command_name.to_s,
          'database_name' => e.database_name,
        )
      }
    end

    # Remove any events from authentication commands.
    events.reject! do |e|
      command_name = e['command_started_event']['command_name']
      %w(saslStart saslContinue authenticate getnonce).include?(command_name)
    end

    events
  end
  module_function :yamlify_command_events

  def convert_operation_options(options)
    if options
      Hash[options.map do |k, v|
        out_v = case k
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
            if out[:level]
              out[:level] = out[:level].to_sym
            end
          end
        when 'causalConsistency'
          out_k = Utils.underscore(k).to_sym
          v
        when 'writeConcern'
          if v == {}
            # Tests added in SPEC-1352 specify {writeConcern: {}} but what
            # they mean is for the driver to use the default write concern,
            # which for Ruby means no write concern is specified at all.
            #
            # This nil return requires the compact call below to get rid of
            # the nils before outgoing options are constructed.
            next nil
          else
            # Write concern option is called :write on the client, but
            # :write_concern on all levels below the client.
            out_k = :write_concern
            # The client expects write concern value to only have symbol keys.
            Hash[v.map do |sub_k, sub_v|
              [sub_k.to_sym, sub_v]
            end]
          end
        else
          raise "Unhandled operation option #{k}"
        end
        [out_k, out_v]
      end.compact]
    else
      {}
    end
  end
  module_function :convert_operation_options

  def int64_value(value)
    if value.respond_to?(:value)
      # bson-ruby >= 4.6.0
      value.value
    else
      value.instance_variable_get('@integer')
    end
  end
  module_function :int64_value

  URI_OPTION_MAP = {
    app_name: 'appName',
    auth_mech: 'authMechanism',
    auth_source: 'authsource',
    replica_set: 'replicaSet',
    ssl_ca_cert: 'tlsCAFile',
    ssl_cert: 'tlsCertificateKeyFile',
    ssl_key: 'tlsCertificateKeyFile',
  }.freeze

  module_function def create_mongodb_uri(address_strs, **opts)
    creds = if opts[:username]
      "#{opts[:username]}:#{opts[:password]}@"
    else
      ''
    end
    uri = "mongodb://#{creds}#{address_strs.join(',')}/"
    if opts[:database]
      uri << opts[:database]
    end
    if uri_options = opts[:uri_options]
      uri << '?'

      uri_options.each do |k, v|
        uri << '&'

        write_k = URI_OPTION_MAP[k] || k

        if k == :compressors
          write_v = v.join(',')
        elsif k == :auth_mech
          if v
            write_v = Mongo::URI::AUTH_MECH_MAP.key(v)
            unless write_v
              raise "Unhandled auth mech value: #{v}"
            end
          else
            next
          end
        else
          write_v = v
        end

        uri << "#{write_k}=#{write_v}"
      end
    end

    uri
  end

  # Client-Side encryption tests introduce the $$type syntax for determining
  # equality in command started events. The $$type key specifies which type of
  # BSON object is expected in the result. If the $$type key is present, only
  # check the class of the result.
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
      same_keys = (expected.keys - actual.keys).empty? &&
        (actual.keys - expected.keys).empty?

      same_values = expected.keys.all? do |key|
        match_with_type?(expected[key], actual[key])
      end

      same_keys && same_values
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
  module_function :match_with_type?

  module_function def get_ec2_metadata_token(ttl: 30, http: nil)
    http ||= Net::HTTP.new('169.254.169.254')
    req = Net::HTTP::Put.new('/latest/api/token',
      # The TTL is required in order to obtain the metadata token.
      {'x-aws-ec2-metadata-token-ttl-seconds' => ttl.to_s})
    resp = http.request(req)
    if resp.code != '200'
      raise "Metadata token request failed: #{e.class}: #{e}"
    end
    resp.body
  end

  module_function def ec2_instance_id
    http = Net::HTTP.new('169.254.169.254')
    metadata_token = get_ec2_metadata_token(http: http)
    req = Net::HTTP::Get.new('/latest/dynamic/instance-identity/document',
      {'x-aws-ec2-metadata-token' => metadata_token})
    resp = http.request(req)
    payload = JSON.parse(resp.body)
    payload.fetch('instanceId')
  end

  module_function def ec2_instance_profile
    http = Net::HTTP.new('169.254.169.254')
    metadata_token = get_ec2_metadata_token(http: http)
    req = Net::HTTP::Get.new('/latest/meta-data/iam/info',
      {'x-aws-ec2-metadata-token' => metadata_token})
    resp = http.request(req)
    if resp.code == '404'
      nil
    else
      payload = JSON.parse(resp.body)
      payload['InstanceProfileArn']
    end
  end

  module_function def wait_for_instance_profile
    deadline = Time.now + 15
    loop do
      begin
        ip = ec2_instance_profile
        if ip
          puts "Instance profile assigned: #{ip}"
          break
        end
      rescue => e
        puts "Problem retrieving instance profile: #{e.class}: #{e}"
      end
      if Time.now >= deadline
        raise 'Instance profile did not get assigned in 15 seconds'
      end
      sleep 3
    end
  end

  module_function def wait_for_no_instance_profile
    deadline = Time.now + 15
    loop do
      begin
        ip = ec2_instance_profile
        if ip.nil?
          puts "Instance profile cleared"
          break
        end
      rescue => e
        puts "Problem retrieving instance profile: #{e.class}: #{e}"
      end
      if Time.now >= deadline
        raise 'Instance profile did not get cleared in 15 seconds'
      end
      sleep 3
    end
  end

  module_function def wrap_forked_child
    begin
      yield
    rescue => e
      STDERR.puts "Failing process #{Process.pid} due to #{e.class}: #{e}"
      exec('/bin/false')
    else
      # Exec so that we do not close any clients etc. in the child.
      exec('/bin/true')
    end
  end

  module_function def subscribe_all(client, subscriber)
    subscribe_all_sdam_proc(subscriber).call(client)
  end

  module_function def subscribe_all_sdam_proc(subscriber)
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
  module_function def get_command_event(client, command_name, include_auth: false)
    subscriber = EventSubscriber.new
    client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    begin
      yield client
    ensure
      client.unsubscribe(Mongo::Monitoring::COMMAND, subscriber)
    end

    subscriber.single_command_started_event(command_name, include_auth: include_auth)
  end
end
