module Mongo
  class PoolManager

    attr_reader :client, :arbiters, :primary, :secondaries, :primary_pool,
      :secondary_pool, :secondary_pools, :hosts, :nodes, :members, :seeds,
      :max_bson_size

    attr_accessor :pinned_pools

    # Create a new set of connection pools.
    #
    # The pool manager will by default use the original seed list passed
    # to the connection objects, accessible via connection.seeds. In addition,
    # the user may pass an additional list of seeds nodes discovered in real
    # time. The union of these lists will be used when attempting to connect,
    # with the newly-discovered nodes being used first.
    def initialize(client, seeds=[])
      @pinned_pools         = {}
      @client               = client
      @seeds                = seeds
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

    def closed?
      pools.all? { |pool| pool.closed? }
    end

    def close(opts={})
      begin
        pools.each { |pool| pool.close(opts) }
      rescue ConnectionFailure
      end
    end

    def read
      read_pool.host_port
    end

    def read_pool(mode=@client.read_preference,
                  tags=@client.tag_sets,
                  acceptable_latency=@client.acceptable_latency)
      if mode == :primary && !tags.empty?
        raise MongoArgumentError, "Read preferecy :primary cannot be combined with tags"
      end

      pinned = @pinned_pools[Thread.current]
      if pinned && pinned.matches_mode(mode) && pinned.matches_tag_sets(tags) && pinned.up?
        pool = pinned
      else
        pool = case mode
        when :primary
          @primary_pool
        when :primary_preferred
          @primary_pool || select_pool(@secondary_pools, tags, acceptable_latency)
        when :secondary
          select_pool(@secondary_pools, tags, acceptable_latency)
        when :secondary_preferred
          select_pool(@secondary_pools, tags, acceptable_latency) || @primary_pool
        when :nearest
          select_pool(pools, tags, acceptable_latency)
        end
      end

      unless pool
        raise ConnectionFailure, "No replica set member available for query " +
          "with read preference matching mode #{mode} and tags matching #{tags}."
      end

      pool
    end

    def pools
      [@primary_pool, *@secondary_pools].compact
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
      @primary          = nil
      @primary_pool     = nil
      @read             = nil
      @read_pool        = nil
      @arbiters         = []
      @secondaries      = []
      @secondary_pool   = nil
      @secondary_pools  = []
      @hosts            = Set.new
      @members          = Set.new
      @refresh_required = false
      @pinned_pools     = {}
    end

    # Connect to each member of the replica set
    # as reported by the given seed node, and return
    # as a list of Mongo::Node objects.
    def connect_to_members
      members = []

      seed = get_valid_seed_node

      seed.node_list.each do |host|
        node = Mongo::Node.new(self.client, host)
        if node.healthy?
          members << node
        end
      end
      seed.close

      if members.empty?
        raise ConnectionFailure, "Failed to connect to any given member."
      end

      members
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
      @primary_pool = Pool.new(self.client, member.host, member.port,
        :size => self.client.pool_size,
        :timeout => self.client.pool_timeout,
        :node => member
      )
    end

    def assign_secondary(member)
      member.last_state = :secondary
      @secondaries << member.host_port
      pool = Pool.new(self.client, member.host, member.port,
        :size => self.client.pool_size,
        :timeout => self.client.pool_timeout,
        :node => member
      )
      @secondary_pools << pool
    end

    def select_pool(candidates, tag_sets, acceptable_latency)
      tag_sets = [tag_sets] unless tag_sets.is_a?(Array)

      if !tag_sets.empty?
        matches = []
        tag_sets.detect do |tag_set|
          matches = candidates.select do |candidate|
            tag_set.none? { |k,v| candidate.tags[k.to_s] != v } &&
            candidate.ping_time
          end
          !matches.empty?
        end
      else
        matches = candidates
      end

      matches.empty? ? nil : near_pool(matches, acceptable_latency)
    end

    def near_pool(pool_set, acceptable_latency)
      nearest_pool = pool_set.min_by { |pool| pool.ping_time }
      near_pools = pool_set.select do |pool|
        (pool.ping_time - nearest_pool.ping_time) <= acceptable_latency
      end
      near_pools[ rand(near_pools.length) ]
    end

    # Iterate through the list of provided seed
    # nodes until we've gotten a response from the
    # replica set we're trying to connect to.
    #
    # If we don't get a response, raise an exception.
    def get_valid_seed_node
      @seeds.each do |seed|
        node = Mongo::Node.new(self.client, seed)
        if !node.connect
          next
        elsif node.set_config && node.healthy?
          return node
        end
      end

      raise ConnectionFailure, "Cannot connect to a replica set using seeds " +
        "#{@seeds.map {|s| "#{s[0]}:#{s[1]}" }.join(', ')}"
    end

    private

    def cache_discovered_seeds(members)
      @seeds = members.map { |n| n.host_port }
    end

  end
end
