module Mongo
  class PoolManager

    attr_reader :connection, :arbiters, :primary, :secondaries, :primary_pool,
      :read_pool, :secondary_pools, :hosts, :nodes, :max_bson_size,
      :tags_to_pools, :tag_map, :members

    # Create a new set of connection pools.
    #
    # The pool manager will by default use the original seed list passed
    # to the connection objects, accessible via connection.seeds. In addition,
    # the user may pass an additional list of seeds nodes discovered in real
    # time. The union of these lists will be used when attempting to connect,
    # with the newly-discovered nodes being used first.
    def initialize(connection, seeds=[])
      @connection = connection
      @original_seeds = connection.seeds
      @seeds = seeds
      @previously_connected = false
    end

    def inspect
      "<Mongo::PoolManager:0x#{self.object_id.to_s(16)} @seeds=#{@seeds}>"
    end

    def connect
      close if @previously_connected

      initialize_data
      members = connect_to_members
      initialize_pools(members)
      cache_discovered_seeds(members)
      set_read_pool
      set_tag_mappings

      @members = members
      @previously_connected = true
    end

    # We're healthy if all members are pingable and if the view
    # of the replica set returned by isMaster is equivalent
    # to our view. If any of these isn't the case,
    # set @refresh_required to true, and return.
    def check_connection_health
      begin
        seed = get_valid_seed_node
      rescue ConnectionFailure
        @refresh_required = true
        return
      end

      config = seed.set_config
      if !config
        @refresh_required = true
        seed.close
        return
      end

      if config['hosts'].length != @members.length
        @refresh_required = true
        seed.close
        return
      end

      config['hosts'].each do |host|
        member = @members.detect do |m|
          m.address == host
        end

        if member && validate_existing_member(member)
          next
        else
          @refresh_required = true
          seed.close
          return false
        end
      end

      seed.close
    end

    # The replica set connection should initiate a full refresh.
    def refresh_required?
      @refresh_required
    end

    def close(opts={})
      begin
        if @primary_pool
          @primary_pool.close(opts)
        end

        if @secondary_pools
          @secondary_pools.each do |pool|
            pool.close(opts)
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

    # The set of nodes that this class has discovered and
    # successfully connected to.
    def seeds
      @seeds || []
    end

    private

    def validate_existing_member(member)
      config = member.set_config
      if !config
        return false
      else
        if member.primary?
          if member.last_state == :primary
            return true
          else # This node is now primary, but didn't used to be.
            return false
          end
        elsif member.last_state == :secondary &&
          member.secondary?
          return true
        else # This node isn't what it used to be.
          return false
        end
      end
    end

    def initialize_data
      @seeds = []
      @primary = nil
      @primary_pool = nil
      @read_pool = nil
      @arbiters = []
      @secondaries = []
      @secondary_pools = []
      @hosts = Set.new
      @members = Set.new
      @tags_to_pools = {}
      @tag_map = {}
      @refresh_required = false
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
          assign_primary(member)
        elsif member.secondary? && !@secondaries.include?(member.host_port)
          assign_secondary(member)
        end
      end

      @max_bson_size = members.first.config['maxBsonObjectSize'] ||
        Mongo::DEFAULT_MAX_BSON_SIZE
      @arbiters = members.first.arbiters
    end

    def assign_primary(member)
      member.last_state = :primary
      @primary = member.host_port
      @primary_pool = Pool.new(self.connection, member.host, member.port,
                              :size => self.connection.pool_size,
                              :timeout => self.connection.pool_timeout,
                              :node => member)
      associate_tags_with_pool(member.tags, @primary_pool)
    end

    def assign_secondary(member)
      member.last_state = :secondary
      @secondaries << member.host_port
      pool = Pool.new(self.connection, member.host, member.port,
                                   :size => self.connection.pool_size,
                                   :timeout => self.connection.pool_timeout,
                                   :node => member)
      @secondary_pools << pool
      associate_tags_with_pool(member.tags, pool)
    end

    # If there's more than one pool associated with
    # a given tag, choose a close one using the bucket method.
    def set_tag_mappings
      @tags_to_pools.each do |key, pool_list|
        if pool_list.length == 1
          @tag_map[key] = pool_list.first
        else
          @tag_map[key] = nearby_pool_from_set(pool_list)
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
      seed_list.each do |seed|
        node = Mongo::Node.new(self.connection, seed)
        if !node.connect
          next
        elsif node.set_config
          return node
        else
          node.close
        end
      end

      raise ConnectionFailure, "Cannot connect to a replica set using seeds " +
        "#{seed_list.map {|s| "#{s[0]}:#{s[1]}" }.join(', ')}"
    end

    def seed_list
      @seeds | @original_seeds
    end

    def cache_discovered_seeds(members)
      @seeds = members.map { |n| n.host_port }
    end

  end
end
