# frozen_string_literal: true
# rubocop:todo all

module ClientRegistryMacros
  def new_local_client(address, options=nil, &block)
    ClientRegistry.instance.new_local_client(address, options, &block)
  end

  def new_local_client_nmio(address, options=nil, &block)
    # Avoid type converting options.
    base_options = {monitoring_io: false}
    if BSON::Document === options || options&.keys&.any? { |key| String === key }
      base_options = Mongo::Options::Redacted.new(base_options)
    end
    options = if options
      base_options.merge(options)
    else
      base_options
    end
    new_local_client(address, options, &block)
  end

  def close_local_clients
    ClientRegistry.instance.close_local_clients
  end
end
