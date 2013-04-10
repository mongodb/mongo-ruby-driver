module Mongo
  class PoolManager
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
                :pools,
                :max_bson_size,
                :max_message_size

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
      @max_bson_size        = DEFAULT_MAX_BSON_SIZE
      @max_message_size     = @max_bson_size * MESSAGE_SIZE_FACTOR
    end

    def inspect
      "<Mongo::PoolManager:0x#{self.object_id.to_s(16)} @seeds=#{@seeds}>"
    end

    def connect
      @refresh_required = false
      disconnect_old_members
      connect_to_members
      initialize_pools(@members)
      @seeds = discovered_seeds
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

      unless current_config = seed.config
        @refresh_required = true
        seed.close
        return
      end

      if current_config['hosts'].length != @members.length
        @refresh_required = true
        seed.close
        return
      end

      current_config['hosts'].each do |host|
        member = @members.detect do |m|
          m.address == host
        end

        if member && validate_existing_member(current_config, member)
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

    def update_max_sizes
      unless @members.size == 0
        @max_bson_size = @members.map(&:max_bson_size).min
        @max_message_size = @members.map(&:max_message_size).min
      end
    end

    private

    def validate_existing_member(current_config, member)
      if current_config['ismaster'] && member.last_state != :primary
        return false
      elsif member.last_state != :other
        return false
      end
      return true
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

    def discovered_seeds
      @members.map(&:host_port)
    end

  end
end
