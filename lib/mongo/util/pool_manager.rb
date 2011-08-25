module Mongo
  class PoolManager

    attr_reader :connection, :seeds, :arbiters, :primary, :secondaries,
      :primary_pool, :read_pool, :secondary_pools, :hosts, :nodes

    def initialize(connection, seeds)
      @connection = connection
      @seeds = seeds
      @refresh_node = nil
    end

    def connect
      initialize_data
      nodes = connect_to_members
      initialize_pools(nodes)
      update_seed_list(nodes)
      @nodes = nodes
    end

    # Ensure that the view of the replica set is current by
    # running the ismaster command and checking to see whether
    # we've connected to all known nodes. If not, automatically
    # connect to these unconnected nodes. This is handy when we've
    # connected to a replica set with no primary or when a secondary
    # node comes up after we've connected.
    #
    # If we're connected to nodes that are no longer part of the set,
    # remove these from our set of secondary pools.
    def update_required?(hosts)
      if !@refresh_node || !@refresh_node.active?
        begin
          @refresh_node = get_valid_seed_node
        rescue ConnectionFailure
          warn "Could not refresh config because no valid seed node was available."
          return
        end
      end
      node = @refresh_node

      node_list = node.node_list

      unconnected_nodes = node_list - hosts
      removed_nodes = hosts - node_list

      if unconnected_nodes.empty? && removed_nodes.empty?
        return false
      else
        {:unconnected => unconnected_nodes, :removed => removed_nodes}
      end
    end

    def update(manager, node_struct)
      reference_manager_data(manager)
      unconnected_nodes = node_struct[:unconnected]
      removed_nodes = node_struct[:removed]

      if !removed_nodes.empty?
        removed_nodes.each do |node|
          if @primary_pool && @primary_pool.host_string == node
            @primary = nil
            @primary_pool.close
            @primary_pool = nil
          elsif rejected_pool = @secondary_pools.reject! {|pool| pool.host_string == node}
            @secondaries.reject! do |secondary|
              secondary.port == rejected_pool.port && secondary.host == rejected_pool.host
            end
          end
        end
      end

      if !unconnected_nodes.empty?
        nodes = []
        unconnected_nodes.each do |host_port|
          node = Mongo::Node.new(self.connection, host_port)
          if node.connect && node.set_config
            nodes << node
          end
        end

        if !nodes.empty?
          initialize_pools(nodes)
        end
      end
    end

    private

    # Note that @arbiters and @read_pool will be
    # assigned automatically.
    def reference_manager_data(manager)
      @primary = manager.primary
      @primary_pool = manager.primary_pool
      @secondaries = manager.secondaries
      @secondary_pools = manager.secondary_pools
      @read_pool = manager.read_pool
      @arbiters = manager.arbiters
      @hosts = manager.hosts
    end

    def initialize_data
      @primary = nil
      @primary_pool = nil
      @read_pool = nil
      @arbiters = []
      @secondaries = []
      @secondary_pools = []
      @hosts = []
      @nodes = []
    end

    # Connect to each member of the replica set
    # as reported by the given seed node, and return
    # as a list of Mongo::Node objects.
    def connect_to_members
      nodes = []

      seed = get_valid_seed_node

      seed.node_list.each do |host|
        node = Mongo::Node.new(self.connection, host)
        if node.connect && node.set_config
          nodes << node
        end
      end

      if nodes.empty?
        raise ConnectionFailure, "Failed to connect to any given member."
      end

      nodes
    end

    # Initialize the connection pools for the primary and secondary nodes.
    def initialize_pools(nodes)
      nodes.each do |member|
        @hosts << member.host_string

        if member.primary?
          @primary = member.host_port
          @primary_pool = Pool.new(self.connection, member.host, member.port,
                                  :size => self.connection.pool_size,
                                  :timeout => self.connection.connect_timeout,
                                  :node => member)
        elsif member.secondary? && !@secondaries.include?(member.host_port)
          @secondaries << member.host_port
          @secondary_pools << Pool.new(self.connection, member.host, member.port,
                                       :size => self.connection.pool_size,
                                       :timeout => self.connection.connect_timeout,
                                       :node => member)
        end
      end

      @arbiters = nodes.first.arbiters
      choose_read_pool
    end

    # Pick a node from the set of possible secondaries.
    # If more than one node is available, use the ping
    # time to figure out which nodes to choose from.
    def choose_read_pool
      if @secondary_pools.empty?
        @read_pool = @primary_pool
      elsif @secondary_pools.size == 1
        @read_pool = @secondary_pools[0]
      else
        ping_ranges = Array.new(3) { |i| Array.new }
        @secondary_pools.each do |pool|
          case pool.ping_time
            when 0..150
              ping_ranges[0] << pool
            when 150..1000
              ping_ranges[1] << pool
            else
              ping_ranges[2] << pool
          end
        end

        for list in ping_ranges do
          break if !list.empty?
        end

        @read_pool = list[rand(list.length)]
      end
    end

    # Iterate through the list of provided seed
    # nodes until we've gotten a response from the
    # replica set we're trying to connect to.
    #
    # If we don't get a response, raise an exception.
    def get_valid_seed_node
      @seeds.each do |seed|
        node = Mongo::Node.new(self.connection, seed)
        if node.connect && node.set_config
          return node
        else
          node.disconnect
        end
      end

      raise ConnectionFailure, "Cannot connect to a replica set using seeds " +
        "#{@seeds.map {|s| "#{s[0]}:#{s[1]}" }.join(', ')}"
    end

    def update_seed_list(nodes)
      @seeds = nodes.map { |n| n.host_port }
    end

  end
end
