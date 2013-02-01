module Mongo
  class PoolManager
    include Mongo::ReadPreference
    include ThreadLocalVariableManager

    attr_reader :client,
                :arbiters,
                :primary,
                :secondaries,
                :primary_pool,
                :secondary_pools,
                :hosts,
                :nodes,
                :members,
                :seeds,
                :pools

    # Create a new set of connection pools.
    #
    # The pool manager will by default use the original seed list passed
    # to the connection objects, accessible via connection.seeds. In addition,
    # the user may pass an additional list of seeds nodes discovered in real
    # time. The union of these lists will be used when attempting to connect,
    # with the newly-discovered nodes being used first.
    def initialize(client, seeds=[])
      @client               = client
      @seeds                = seeds

      @pools                = Set.new
      @primary              = nil
      @primary_pool         = nil
      @secondaries          = Set.new
      @secondary_pools      = []
      @hosts                = Set.new
      @members              = Set.new
      @refresh_required     = false
    end

    def inspect
      "<Mongo::PoolManager:0x#{self.object_id.to_s(16)} @seeds=#{@seeds}>"
    end

    def connect
      @refresh_required = false
      disconnect_old_members
      connect_to_members
      initialize_pools(@members)
      cache_discovered_seeds
    end

    def refresh!(additional_seeds)
      @seeds |= additional_seeds
      connect
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

      if !seed.config
        @refresh_required = true
        seed.close
        return
      end

      if seed.config['hosts'].length != @members.length
        @refresh_required = true
        seed.close
        return
      end

      seed.config['hosts'].each do |host|
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

    def read_pool(mode=@client.read,
                  tags=@client.tag_sets,
                  acceptable_latency=@client.acceptable_latency)

      pinned = thread_local[:pinned_pools][self.object_id]

      if pinned && pinned.matches_mode(mode) && pinned.matches_tag_sets(tags) && pinned.up?
        pool = pinned
      else
        pool = select_pool(mode, tags, acceptable_latency)
      end

      unless pool
        raise ConnectionFailure, "No replica set member available for query " +
          "with read preference matching mode #{mode} and tags matching #{tags}."
      end

      pool
    end

    def max_bson_size
      @max_bson_size ||= config_min('maxBsonObjectSize', DEFAULT_MAX_BSON_SIZE)
    end

    def max_message_size
      @max_message_size ||= config_min('maxMessageSizeBytes', DEFAULT_MAX_MESSAGE_SIZE)
    end

    private

    def validate_existing_member(member)
      if !member.config
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

    # For any existing members, close and remove any that are unhealthy or already closed.
    def disconnect_old_members
      @pools.reject!   {|pool| !pool.healthy? }
      @members.reject! {|node| !node.healthy? }
    end

    # Connect to each member of the replica set
    # as reported by the given seed node.
    def connect_to_members
      seed = get_valid_seed_node
      seed.node_list.each do |host|
        if existing = @members.detect {|node| node =~ host }
          if existing.healthy?
            # Refresh this node's configuration
            existing.set_config
            # If we are unhealthy after refreshing our config, drop from the set.
            if !existing.healthy?
              @members.delete existing
            else
              next
            end
          else
            existing.close
            @members.delete existing
          end
        end

        node = Mongo::Node.new(self.client, host)
        node.connect
        @members << node if node.healthy?
      end
      seed.close

      if @members.empty?
        raise ConnectionFailure, "Failed to connect to any given member."
      end
    end

    # Initialize the connection pools for the primary and secondary nodes.
    def initialize_pools(members)
      @primary_pool = nil
      @primary = nil
      @secondaries.clear
      @secondary_pools.clear
      @hosts.clear

      members.each do |member|
        member.last_state = nil
        @hosts << member.host_string
        if member.primary?
          assign_primary(member)
        elsif member.secondary?
          # member could be not primary but secondary still is false
          assign_secondary(member)
        end
      end

      @arbiters = members.first.arbiters
    end

    def config_min(attribute, default)
      @members.reject {|m| !m.config[attribute]}
      @members.map {|m| m.config[attribute]}.min || default
    end

    def assign_primary(member)
      member.last_state = :primary
      @primary = member.host_port
      if existing = @pools.detect {|pool| pool.node == member }
        @primary_pool = existing
      else
        @primary_pool = Pool.new(self.client, member.host, member.port,
          :size => self.client.pool_size,
          :timeout => self.client.pool_timeout,
          :node => member
        )
        @pools << @primary_pool
      end
    end

    def assign_secondary(member)
      member.last_state = :secondary
      @secondaries << member.host_port
      if existing = @pools.detect {|pool| pool.node == member }
        @secondary_pools << existing
      else
        pool = Pool.new(self.client, member.host, member.port,
          :size => self.client.pool_size,
          :timeout => self.client.pool_timeout,
          :node => member
        )
        @secondary_pools << pool
        @pools << pool
      end
    end

    # Iterate through the list of provided seed
    # nodes until we've gotten a response from the
    # replica set we're trying to connect to.
    #
    # If we don't get a response, raise an exception.
    def get_valid_seed_node
      @seeds.each do |seed|
        node = Mongo::Node.new(self.client, seed)
        node.connect
        return node if node.healthy?
      end

      raise ConnectionFailure, "Cannot connect to a replica set using seeds " +
        "#{@seeds.map {|s| "#{s[0]}:#{s[1]}" }.join(', ')}"
    end

    private

    def cache_discovered_seeds
      @seeds = @members.map &:host_port
    end

  end
end
