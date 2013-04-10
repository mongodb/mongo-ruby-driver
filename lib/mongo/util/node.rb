module Mongo
  class Node

    attr_accessor :host, :port, :address, :client, :socket, :last_state

    def initialize(client, host_port)
      @client = client
      @manager = @client.local_manager
      @host, @port = Support.normalize_seeds(host_port)
      @address = "#{@host}:#{@port}"
      @config = nil
      @socket = nil
      @node_mutex = Mutex.new
    end

    def eql?(other)
      (other.is_a?(Node) && @address == other.address)
    end
    alias :== :eql?

    def =~(other)
      if other.is_a?(String)
        h, p = Support.normalize_seeds(other)
        h == @host && p == @port
      else
        false
      end
    end

    def host_string
      address
    end

    def config
      connect unless connected?
      set_config unless @config || !connected?
      @config
    end

    def inspect
      "<Mongo::Node:0x#{self.object_id.to_s(16)} @host=#{@host} @port=#{@port}>"
    end

    # Create a connection to the provided node,
    # and, if successful, return the socket. Otherwise,
    # return nil.
    def connect
      @node_mutex.synchronize do
        begin
          @socket = @client.socket_class.new(@host, @port,
            @client.op_timeout, @client.connect_timeout
          )
        rescue OperationTimeout, ConnectionFailure, OperationFailure, SocketError, SystemCallError, IOError => ex
          @client.log(:debug, "Failed connection to #{host_string} with #{ex.class}, #{ex.message}.")
          close
        end
      end
    end

    # This should only be called within a mutex
    def close
      if @socket && !@socket.closed?
        @socket.close
      end
      @socket = nil
      @config = nil
    end

    def connected?
      @socket != nil && !@socket.closed?
    end

    def active?
      begin
        result = @client['admin'].command({:ping => 1}, :socket => @socket)
      rescue OperationFailure, SocketError, SystemCallError, IOError
        return nil
      end
      result['ok'] == 1
    end

    # Get the configuration for the provided node as returned by the
    # ismaster command. Additionally, check that the replica set name
    # matches with the name provided.
    def set_config
      @node_mutex.synchronize do
        begin
          if @config
            @last_state = @config['ismaster'] ? :primary : :other
          end

          @config = @client['admin'].command({:ismaster => 1}, :socket => @socket)

          if @config['msg']
            @client.log(:warn, "#{config['msg']}")
          end

          unless @client.mongos?
            check_set_membership(@config)
            check_set_name(@config)
          end
        rescue ConnectionFailure, OperationFailure, OperationTimeout, SocketError, SystemCallError, IOError => ex
          @client.log(:warn, "Attempted connection to node #{host_string} raised " +
                              "#{ex.class}: #{ex.message}")
          # Socket may already be nil from issuing command
          close
        end
      end
      @manager.update_max_sizes
    end

    # Return a list of replica set nodes from the config.
    # Note: this excludes arbiters.
    def node_list
      nodes = []
      nodes += config['hosts'] if config['hosts']
      nodes += config['passives'] if config['passives']
      nodes += ["#{@host}:#{@port}"] if @client.mongos?
      nodes
    end

    def arbiters
      return [] unless config['arbiters']
      config['arbiters'].map do |arbiter|
        Support.normalize_seeds(arbiter)
      end
    end

    def primary?
      config['ismaster'] == true || config['ismaster'] == 1
    end

    def secondary?
      config['secondary'] == true || config['secondary'] == 1
    end

    def tags
      config['tags'] || {}
    end

    def host_port
      [@host, @port]
    end

    def hash
      address.hash
    end

    def healthy?
      connected? && config
    end

    def max_bson_size
      max = config['maxBsonObjectSize'] || DEFAULT_MAX_BSON_SIZE
    end

    def max_message_size
      max = config['maxMessageSizeBytes'] || max_bson_size * MESSAGE_SIZE_FACTOR
    end

    protected

    # Ensure that this node is a healthy member of a replica set.
    def check_set_membership(config)
      if !config.has_key?('hosts')
        message = "Will not connect to #{host_string} because it's not a member " +
          "of a replica set."
        raise ConnectionFailure, message
      elsif config['hosts'].length == 1 && !config['ismaster'] &&
        !config['secondary']
        message = "Attempting to connect to an unhealthy, single-node replica set."
        raise ConnectionFailure, message
      end
    end

    # Ensure that this node is part of a replica set of the expected name.
    def check_set_name(config)
      if @client.replica_set_name
        if !config['setName']
          @client.log(:warn, "Could not verify replica set name for member #{host_string} " +
            "because ismaster does not return name in this version of MongoDB")
        elsif @client.replica_set_name != config['setName']
          message = "Attempting to connect to replica set '#{config['setName']}' on member #{host_string} " +
            "but expected '#{@client.replica_set_name}'"
          raise ReplicaSetConnectionError, message
        end
      end
    end
  end
end
