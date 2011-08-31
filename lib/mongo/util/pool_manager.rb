module Mongo
  class PoolManager

    attr_reader :connection, :seeds, :arbiters, :primary, :secondaries,
      :primary_pool, :read_pool, :secondary_pools, :hosts, :nodes, :max_bson_size,
      :tags_to_pools

    def initialize(connection, seeds)
      @connection = connection
      @seeds = seeds
      @refresh_node = nil
    end

    def connect
      initialize_data
      members = connect_to_members
      initialize_pools(members)
      update_seed_list(members)
      @members = members
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
      if !@refresh_node || !@refresh_node.set_config
        begin
          @refresh_node = get_valid_seed_node
        rescue ConnectionFailure
          warn "Could not refresh config because no valid seed node was available."
          return
        end
      end

      hosts != @refresh_node.node_list
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
          elsif rejected_pool = @secondary_pools.detect {|pool| pool.host_string == node}
            @secondary_pools.delete(rejected_pool)
            @secondaries.delete(rejected_pool.host_port)
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

    def reference_manager_data(manager)
      @primary = manager.primary
      @primary_pool = manager.primary_pool
      @secondaries = manager.secondaries
      @secondary_pools = manager.secondary_pools
      @read_pool = manager.read_pool
      @arbiters = manager.arbiters
      @hosts = manager.hosts
      @tags_to_pools = {}
    end

    def initialize_data
      @primary = nil
      @primary_pool = nil
      @read_pool = nil
      @arbiters = []
      @secondaries = []
      @secondary_pools = []
      @hosts = Set.new
      @members = Set.new
      @tags_to_pools = {}
    end

    # Connect to each member of the replica set
    # as reported by the given seed node, and return
    # as a list of Mongo::Node objects.
    def connect_to_members
      members = []

      seed = get_valid_seed_node

      seed.node_list.each do |host|
        node = Mongo::Node.new(self.connection, host)
        if node.connect && node.set_config
          members << node
        end
      end

      if members.empty?
        raise ConnectionFailure, "Failed to connect to any given member."
      end

      members
    end

    def associate_tags_with_pool(tags, pool)
      tags.each_key do |key|
        @tags_to_pools[{key => tags[key]}] ||= []
        @tags_to_pools[{key => tags[key]}] << pool
      end
    end

    # Sort each tag pool entry in descending order
    # according to ping time.
    def sort_tag_pools!
      @tags_to_pools.each_value do |pool_list|
        pool_list.sort! do |a, b|
          a.ping_time <=> b.ping_time
        end
      end
    end

    # Initialize the connection pools for the primary and secondary nodes.
    def initialize_pools(members)
      members.each do |member|
        @hosts << member.host_string

        if member.primary?
          @primary = member.host_port
          @primary_pool = Pool.new(self.connection, member.host, member.port,
                                  :size => self.connection.pool_size,
                                  :timeout => self.connection.connect_timeout,
                                  :node => member)
          associate_tags_with_pool(member.tags, @primary_pool)
        elsif member.secondary? && !@secondaries.include?(member.host_port)
          @secondaries << member.host_port
          pool = Pool.new(self.connection, member.host, member.port,
                                       :size => self.connection.pool_size,
                                       :timeout => self.connection.connect_timeout,
                                       :node => member)
          @secondary_pools << pool
          associate_tags_with_pool(member.tags, pool)
        end
      end

      sort_tag_pools!
      @max_bson_size = members.first.config['maxBsonObjectSize'] ||
        Mongo::DEFAULT_MAX_BSON_SIZE
      @arbiters = members.first.arbiters
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

    def update_seed_list(members)
      @seeds = members.map { |n| n.host_port }
    end

  end
end
