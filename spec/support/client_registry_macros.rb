module ClientRegistryMacros
  def new_local_client(address, options=nil)
    ClientRegistry.instance.new_local_client(address, options)
  end

  def new_local_client_nmio(address, options=nil)
    # Avoid type converting options.
    base_options = if Hash === options
      if options['monitoring_io']
        {'monitoring_io' => false}
      else
        {monitoring_io: false}
      end
    else
      {monitoring_io: false}
    end
    if options
      options = options.class.new(base_options).update(options)
    else
      options = base_options
    end
    new_local_client(address, options)
  end

  def close_local_clients
    ClientRegistry.instance.close_local_clients
  end
end
