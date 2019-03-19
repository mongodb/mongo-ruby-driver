module Utils

  # Converts a 'camelCase' string or symbol to a :snake_case symbol.
  def camel_to_snake(ident)
    ident = ident.is_a?(String) ? ident.dup : ident.to_s
    ident[0] = ident[0].downcase
    ident.chars.reduce('') { |s, c| s + (/[A-Z]/ =~ c ? "_#{c.downcase}" : c) }.to_sym
  end
  module_function :camel_to_snake

  # Creates a copy of a hash where all keys and string values are converted to snake-case symbols.
  # For example, `{ 'fooBar' => { 'baz' => 'bingBing', :x => 1 } }` converts to
  # `{ :foo_bar => { :baz => :bing_bing, :x => 1 } }`.
  def snakeize_hash(value)
    return camel_to_snake(value) if value.is_a?(String)
    return value unless value.is_a?(Hash)

    value.reduce({}) do |hash, kv|
      hash.tap do |h|
        h[camel_to_snake(kv.first)] = snakeize_hash(kv.last)
      end
    end
  end
  module_function :snakeize_hash

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
        kv[0] = camel_to_snake(kv[0])
      end

      opts.tap { |o| o[kv.first] = kv.last }
    end
  end
  module_function :convert_client_options
end
