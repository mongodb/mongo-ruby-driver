module Utils

  # Converts a 'camelCase' string or symbol to a :under_score symbol.
  def underscore(str)
    str = str.to_s
    str = str[0].downcase + str[1...str.length].gsub(/([A-Z]+)/) { |m| "_#{m.downcase}" }
    str.to_sym
  end
  module_function :underscore

  # Creates a copy of a hash where all keys and string values are converted to snake-case symbols.
  # For example, `{ 'fooBar' => { 'baz' => 'bingBing', :x => 1 } }` converts to
  # `{ :foo_bar => { :baz => :bing_bing, :x => 1 } }`.
  def snakeize_hash(value)
    return underscore(value) if value.is_a?(String)
    return value unless value.is_a?(Hash)

    value.reduce({}) do |hash, kv|
      hash.tap do |h|
        h[underscore(kv.first)] = snakeize_hash(kv.last)
      end
    end
  end
  module_function :snakeize_hash

  def camelize(str, upcase_first = true)
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
    spec_test_options.reduce({}) do |opts, kv|
      case kv.first
      when 'readConcernLevel'
        kv = [:read_concern, { 'level' => kv.last }]
      when 'readPreference'
        kv = [:read, { 'mode' => kv.last }]
      when 'w'
        kv = [:write, { w: kv.last }]
      else
        kv[0] = underscore(kv[0])
      end

      opts.tap { |o| o[kv.first] = kv.last }
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
          '$numberLong' => command['txnNumber'].instance_variable_get(:@integer).to_s
        }
      end

      # Fake $code for map/reduce commands
      %w(map reduce).each do |key|
        if command[key].is_a?(String)
          command[key] = {'$code' => command[key]}
        end
      end

      # The spec files don't include these fields, so we delete them.
      command.delete('$readPreference')
      command.delete('bypassDocumentValidation')
      command.delete('$clusterTime')

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
    events.reject! { |e| e['command_started_event']['command_name'].start_with?('sasl') }

    events
  end
  module_function :yamlify_command_events
end
