module ClientRegistryMacros
  def new_local_client(address, options=nil)
    ClientRegistry.instance.new_local_client(address, options)
  end

  def new_local_client_nmio(address, options=nil)
    new_local_client(address, Mongo::Options::Redacted.new(
      monitoring_io: false).merge(options || {}))
  end

  def close_local_clients
    ClientRegistry.instance.close_local_clients
  end
end
