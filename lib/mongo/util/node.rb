module Mongo
  class Node

    attr_accessor :host, :port, :address, :config, :connection, :socket,
      :last_state

    def initialize(connection, data)
      @connection = connection
      if data.is_a?(String)
        @host, @port = split_nodes(data)
      else
        @host = data[0]
        @port = data[1].nil? ? Connection::DEFAULT_PORT : data[1].to_i
      end
      @address = "#{host}:#{port}"
      @config = nil
      @socket = nil
    end

    def eql?(other)
      other.is_a?(Node) && host == other.host && port == other.port
    end
    alias :== :eql?

    def host_string
      address
    end

    def inspect
      "<Mongo::Node:0x#{self.object_id.to_s(16)} @host=#{@host} @port=#{@port}>"
    end

    # Create a connection to the provided node,
    # and, if successful, return the socket. Otherwise,
    # return nil.
    def connect
      begin
        socket = nil
        if @connection.connect_timeout
          Mongo::TimeoutHandler.timeout(@connection.connect_timeout, OperationTimeout) do
            socket = @connection.socket_class.new(@host, @port)
          end
        else
          socket = @connection.socket_class.new(@host, @port)
        end

        if socket.nil?
          return nil
        else
          socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
        end
      rescue OperationTimeout, OperationFailure, SocketError, SystemCallError, IOError => ex
        @connection.log(:debug, "Failed connection to #{host_string} with #{ex.class}, #{ex.message}.")
        socket.close if socket
        return nil
      end

      @socket = socket
    end

    def close
      if @socket
        @socket.close
        @socket = nil
        @config = nil
      end
    end

    def connected?
      @socket != nil
    end

    def active?
      begin
        result = @connection['admin'].command({:ping => 1}, :socket => @socket)
        return result['ok'] == 1
      rescue OperationFailure, SocketError, SystemCallError, IOError => ex
        return nil
      end
    end

    # Get the configuration for the provided node as returned by the
    # ismaster command. Additionally, check that the replica set name
    # matches with the name provided.
    def set_config
      begin
        @config = @connection['admin'].command({:ismaster => 1}, :socket => @socket)

        if @config['msg'] && @logger
          @connection.log(:warn, "#{config['msg']}")
        end

        check_set_membership(config)
        check_set_name(config)
      rescue ConnectionFailure, OperationFailure, SocketError, SystemCallError, IOError => ex
        @connection.log(:warn, "Attempted connection to node #{host_string} raised " +
                            "#{ex.class}: #{ex.message}")
        return nil
      end

      @config
    end

    # Return a list of replica set nodes from the config.
    # Note: this excludes arbiters.
    def node_list
      connect unless connected?
      set_config unless @config

      return [] unless config

      nodes = []
      nodes += config['hosts'] if config['hosts']
      nodes += config['passives'] if config['passives']
      nodes
    end

    def arbiters
      connect unless connected?
      set_config unless @config
      return [] unless config['arbiters']

      config['arbiters'].map do |arbiter|
        split_nodes(arbiter)
      end
    end

    def tags
      connect unless connected?
      set_config unless @config
      return {} unless config['tags'] && !config['tags'].empty?

      config['tags']
    end

    def primary?
      @config['ismaster'] == true || @config['ismaster'] == 1
    end

    def secondary?
      @config['secondary'] == true || @config['secondary'] == 1
    end

    def host_port
      [@host, @port]
    end

    def hash
      address.hash
    end

    private

    def split_nodes(host_string)
      data = host_string.split(":")
      host = data[0]
      port = data[1].nil? ? Connection::DEFAULT_PORT : data[1].to_i

      [host, port]
    end

    # Ensure that this node is a member of a replica set.
    def check_set_membership(config)
      if !config['hosts']
        message = "Will not connect to #{host_string} because it's not a member " +
          "of a replica set."
        raise ConnectionFailure, message
      end
    end

    # Ensure that this node is part of a replica set of the expected name.
    def check_set_name(config)
      if @connection.replica_set_name
        if !config['setName']
          @connection.log(:warn, "Could not verify replica set name for member #{host_string} " +
            "because ismaster does not return name in this version of MongoDB")
        elsif @connection.replica_set_name != config['setName']
          message = "Attempting to connect to replica set '#{config['setName']}' on member #{host_string} " +
            "but expected '#{@connection.replica_set_name}'"
          raise ReplicaSetConnectionError, message
        end
      end
    end
  end
end
