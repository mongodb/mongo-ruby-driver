module ClientRegistryMacros
  def new_local_client(*args)
    ClientRegistry.instance.new_local_client(*args)
  end

  def close_local_clients
    ClientRegistry.instance.close_local_clients
  end
end
