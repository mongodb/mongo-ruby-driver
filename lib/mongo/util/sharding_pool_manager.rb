
module Mongo
  module ShardingNode
    def set_config
      begin
        @config = @client['admin'].command({:ismaster => 1}, :socket => @socket)

        # warning: instance variable @logger not initialized
        #if @config['msg'] && @logger
        #  @client.log(:warn, "#{config['msg']}")
        #end

      rescue ConnectionFailure, OperationFailure, OperationTimeout, SocketError, SystemCallError, IOError => ex
        @client.log(:warn, "Attempted connection to node #{host_string} raised " +
            "#{ex.class}: #{ex.message}")

        # Socket may already be nil from issuing command
        if @socket && !@socket.closed?
          @socket.close
        end

        return nil
      end

      @config
    end

    # Return a list of sharded cluster nodes from the config - currently just the current node.
    def node_list
      connect unless connected?
      set_config unless @config

      return [] unless config

      ["#{@host}:#{@port}"]
    end

  end

  class ShardingPoolManager < PoolManager

    attr_reader :client, :primary, :primary_pool, :hosts, :nodes,
      :max_bson_size, :members

    # Create a new set of connection pools.
    #
    # The pool manager will by default use the original seed list passed
    # to the connection objects, accessible via connection.seeds. In addition,
    # the user may pass an additional list of seeds nodes discovered in real
    # time. The union of these lists will be used when attempting to connect,
    # with the newly-discovered nodes being used first.
    def initialize(client, seeds=[])
      super
    end

    def inspect
      "<Mongo::ShardingPoolManager:0x#{self.object_id.to_s(16)} @seeds=#{@seeds}>"
    end

    # "Best" should be the member with the fastest ping time
    # but connect/connect_to_members reinitializes @members
    def best(members)
      Array(members.first)
    end

    def connect
      close if @previously_connected

      initialize_data
      members = connect_to_members
      initialize_pools(best(members))

      @members = members
      @previously_connected = true
    end

    # We want to refresh to the member with the fastest ping time
    # but also want to minimize refreshes
    # We're healthy if the primary is pingable. If this isn't the case,
    # or the members have changed, set @refresh_required to true, and return.
    # The config.mongos find can't be part of the connect call chain due to infinite recursion
    def check_connection_health
      begin
        seeds = @client['config']['mongos'].find.to_a.map{|doc| doc['_id']}
        if @seeds != seeds
          @seeds = seeds
          @refresh_required = true
        end
      rescue Mongo::OperationFailure
        @refresh_required = true
      end
    end

    private

    # Connect to each member of the sharded cluster
    # as reported by the given seed node, and return
    # as a list of Mongo::Node objects.
    def connect_to_members
      members = []

      seed = get_valid_seed_node

      seed.node_list.each do |host|
        node = Mongo::Node.new(self.client, host)
        node.extend ShardingNode
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

    # Iterate through the list of provided seed
    # nodes until we've gotten a response from the
    # sharded cluster we're trying to connect to.
    #
    # If we don't get a response, raise an exception.
    def get_valid_seed_node
      @seeds.each do |seed|
        node = Mongo::Node.new(self.client, seed)
        node.extend ShardingNode
        if !node.connect
          next
        elsif node.set_config && node.healthy?
          return node
        end
      end

      raise ConnectionFailure, "Cannot connect to a sharded cluster using seeds " +
          "#{@seeds.map {|s| "#{s[0]}:#{s[1]}" }.join(', ')}"
    end

  end
end
