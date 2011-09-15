module Mongo
  class PoolManager

    attr_reader :connection, :seeds, :arbiters, :primary, :secondaries,
      :primary_pool, :read_pool, :secondary_pools, :hosts, :nodes, :max_bson_size,
      :tags_to_pools, :members

    def initialize(connection, seeds)
      @connection = connection
      @seeds = seeds
      @refresh_node = nil
      @previously_connected = false
    end

    def inspect
      "<Mongo::PoolManager:0x#{self.object_id.to_s(16)} @seeds=#{@seeds}>"
    end

    def connect
      if @previously_connected
        close
      end

      initialize_data
      members = connect_to_members
      initialize_pools(members)
      update_seed_list(members)

      @members = members
      @previously_connected = true
    end

    def healthy?
      if !@refresh_node || !refresh_node.set_config
        return false
      end

      #if refresh_node.node_list
    end

    def close
      begin
        if @primary_pool
          @primary_pool.close
        end

        if @secondary_pools
          @secondary_pools.each do |pool|
            pool.close
          end
        end

        if @members
          @members.each do |member|
            member.close
          end
        end

        rescue ConnectionFailure
      end
    end

    private

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
      seed.close

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


      @max_bson_size = members.first.config['maxBsonObjectSize'] ||
        Mongo::DEFAULT_MAX_BSON_SIZE
      @arbiters = members.first.arbiters

      set_read_pool
      set_primary_tag_pools
    end

    # If there's more than one pool associated with
    # a given tag, choose a close one using the bucket method.
    def set_primary_tag_pools
      @tags_to_pools.each do |k, pool_list|
        if pool_list.length == 1
          @tags_to_pools[k] = pool_list.first
        else
          @tags_to_pools[k] = nearby_pool_from_set(pool_list)
        end
      end
    end

    # Pick a node from the set of possible secondaries.
    # If more than one node is available, use the ping
    # time to figure out which nodes to choose from.
    def set_read_pool
      if @secondary_pools.empty?
         @read_pool = @primary_pool
      elsif @secondary_pools.size == 1
         @read_pool = @secondary_pools[0]
      else
        @read_pool = nearby_pool_from_set(@secondary_pools)
      end
    end

    def nearby_pool_from_set(pool_set)
      ping_ranges = Array.new(3) { |i| Array.new }
        pool_set.each do |pool|
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

      list[rand(list.length)]
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
          node.close
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
