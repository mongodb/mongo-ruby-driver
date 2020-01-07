require 'base64'

module Utils
  # A limited extended JSON parser to use in FLE specs.
  # DO NOT consider this a complete extended JSON parser.
  # This method will be removed once a complete extended
  # JSON parser has been implemented.
  #
  # This method may modify the provided argument
  def parse_extended_json(doc)
    requires_parsing = doc.is_a?(Hash) && (doc.key?('$binary') || doc.key?('$date') || doc.key?('$numberInt'))
    return parse_json_object(doc) if requires_parsing

    if doc.is_a?(Hash)
      return doc.each do |key, value|
        doc[key] = parse_extended_json(value)
      end
    end

    if doc.is_a?(Array)
      return doc.each.with_index do |el, idx|
        doc[idx] = parse_extended_json(el)
      end
    end

    return doc
  end
  module_function :parse_extended_json

  def parse_json_object(obj)
    raise "JSON object must be a Hash" unless obj.is_a?(Hash)

    if obj.key?('$binary')
      data = Base64.decode64(obj['$binary']['base64'])
      subtype = obj['$binary']['subType'].to_i == 4 ? :uuid : :generic

      BSON::Binary.new(data, subtype)
    elsif obj.key?('$date')
      time = obj['$date']['$numberLong']
      DateTime.strptime(time, '%Q')
    elsif obj.key?('$numberInt')
      obj['$numberInt'].to_i
    end
  end
  module_function :parse_json_object
  private :parse_json_object

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

  # Converts camel case clientOptions, as used in spec tests,
  # to Ruby driver underscore options.
  def convert_client_options(spec_test_options)
    uri = Mongo::URI.new('mongodb://localhost')
    spec_test_options.reduce({}) do |opts, (name, value)|
      uri.send(:add_uri_option, name, value.to_s, opts)
      opts
    end
  end
  module_function :convert_client_options

  def order_hash(hash)
    Hash[hash.to_a.sort]
  end
  module_function :order_hash

  # Transforms an array of CommandStarted events to an array of hashes
  # matching event specification in YAML spec files
  def yamlify_command_events(events)
    events = events.map do |e|
      command = e.command.dup

      # Convert txnNumber field from a BSON integer to an extended JSON int64
      if command['txnNumber']
        command['txnNumber'] = {
          '$numberLong' => int64_value(command['txnNumber']).to_s
        }
      end

      # Fake $code for map/reduce commands
      %w(map reduce).each do |key|
        if command[key].is_a?(String)
          command[key] = {'$code' => command[key]}
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
      command['getMore'] = { '$numberLong' => '42' } if command['getMore']

      # Remove fields if empty
      #command.delete('filter') if command['filter'] && command['filter'].empty?
      command.delete('query') if command['query'] && command['query'].empty?

      if filter = command['filter']
        # Since the Ruby driver does not implement extended JSON, hack
        # the types here manually.
        # Note that this code mutates the command.
        %w(_id files_id).each do |key|
          if filter[key] && filter[key].is_a?(BSON::ObjectId)
            filter[key] = {'$oid' => filter[key].to_s}
          end
        end
      end

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
      command_name.start_with?('sasl') || command_name == 'authenticate'
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
          # Write concern option is called :write on the client, but
          # :write_concern on all levels below the client.
          out_k = :write_concern
          # The client expects write concern value to only have symbol keys.
          Hash[v.map do |sub_k, sub_v|
            [sub_k.to_sym, sub_v]
          end]
        else
          raise "Unhandled operation option #{k}"
        end
        [out_k, out_v]
      end]
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
end
