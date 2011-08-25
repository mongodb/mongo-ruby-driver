module Mongo
  class Node

    attr_accessor :host, :port, :address, :config, :repl_set_status, :connection, :socket

    def initialize(connection, data)
      self.connection = connection
      if data.is_a?(String)
        self.host, self.port = split_nodes(data)
      else
        self.host, self.port = data
      end
      self.address = "#{host}:#{port}"
    end

    def eql?(other)
      other.is_a?(Node) && host == other.host && port == other.port
    end
    alias :== :eql?

    def host_string
      "#{@host}:#{@port}"
    end

    # Create a connection to the provided node,
    # and, if successful, return the socket. Otherwise,
    # return nil.
    def connect
      begin
        socket = nil
        if self.connection.connect_timeout
          Mongo::TimeoutHandler.timeout(self.connection.connect_timeout, OperationTimeout) do
            socket = TCPSocket.new(self.host, self.port)
          end
        else
          socket = TCPSocket.new(self.host, self.port)
        end

        if socket.nil?
          return nil
        else
          socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
        end
      rescue OperationTimeout, OperationFailure, SocketError, SystemCallError, IOError => ex
        self.connection.log(:debug, "Failed connection to #{host_string} with #{ex.class}, #{ex.message}.")
        socket.close if socket
        return nil
      end

      self.socket = socket
    end

    def disconnect
      if self.socket
        self.socket.close
        self.socket = nil
        self.config = nil
      end
    end

    def connected?
      self.socket != nil
    end

    def active?
      begin
        result = self.connection['admin'].command({:ping => 1}, :socket => self.socket)
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
        self.config = self.connection['admin'].command({:ismaster => 1}, :socket => self.socket)

        if self.config['msg'] && @logger
          self.connection.logger.warn("MONGODB #{config['msg']}")
        end

        check_set_name
      rescue OperationFailure, SocketError, SystemCallError, IOError => ex
        return nil
      end

      self.config
    end

    # Return a list of replica set nodes from the config.
    # Note: this excludes arbiters.
    def node_list
      connect unless connected?
      set_config

      return [] unless config

      nodes = []
      nodes += config['hosts'] if config['hosts']
      nodes += config['passives'] if config['passives']
      nodes
    end

    def arbiters
      connect unless connected?
      return [] unless config['arbiters']

      config['arbiters'].map do |arbiter|
        split_nodes(arbiter)
      end
    end

    def primary?
      self.config['ismaster'] == true || self.config['ismaster'] == 1
    end

    def secondary?
      self.config['secondary'] == true || self.config['secondary'] == 1
    end

    def host_port
      [self.host, self.port]
    end

    def hash
      address.hash
    end

    private

    def split_nodes(host_string)
      data = host_string.split(":")
      host = data[0]
      port = data[1].to_i || Connection::DEFAULT_PORT

      [host, port]
    end

    # Make sure that we're connected to the expected replica set.
    def check_set_name
      if self.connection.replica_set_name
        if !self.config['setName']
          self.connection.logger.warn("MONGODB [warning] could not verify replica set name " +
            "because ismaster does not return name in this version of MongoDB")
        elsif self.connection.replica_set_name != self.config['setName']
          raise ReplicaSetConnectionError,
            "Attempting to connect to replica set '#{config['setName']}' " +
            "but expected '#{self.connection.replica_set_name}'"
        end
      end
    end
  end
end
