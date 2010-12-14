# encoding: UTF-8

# --
# Copyright (C) 2008-2010 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

module Mongo

  # Instantiates and manages connections to MongoDB.
  class ReplSetConnection < Connection
    attr_reader :nodes, :secondaries, :arbiters, :read_pool, :secondary_pools

    def initialize(*args)
      if args.last.is_a?(Hash)
        opts = args.pop
      else
        opts = {}
      end

      unless args.length > 0
        raise MongoArgumentError, "A ReplSetConnection requires at least one node."
      end

      # Get seed nodes
      @nodes = args

      # Replica set name
      @replica_set = opts[:rs_name]

      # Cache the various node types when connecting to a replica set.
      @secondaries = []
      @arbiters    = []

      # Connection pools for each secondary node
      @secondary_pools = []
      @read_pool = nil

      # Are we allowing reads from secondaries?
      @read_secondary = opts.fetch(:read_secondary, false)

      setup(opts)
    end

    # Create a new socket and attempt to connect to master.
    # If successful, sets host and port to master and returns the socket.
    #
    # If connecting to a replica set, this method will replace the
    # initially-provided seed list with any nodes known to the set.
    #
    # @raise [ConnectionFailure] if unable to connect to any host or port.
    def connect
      reset_connection
      @nodes_to_try = @nodes.clone

      while connecting?
        node   = @nodes_to_try.shift
        config = check_is_master(node)

        if is_primary?(config)
          set_primary(node)
        else
          set_auxillary(node, config)
        end
      end

      pick_secondary_for_read if @read_secondary

      if !connected?
        if @secondary_pools.empty?
          raise ConnectionFailure, "Failed to connect any given host:port"
        else
          raise ConnectionFailure, "Failed to connect to primary node."
        end
      end
    end

    def connecting?
      @nodes_to_try.length > 0
    end

    # Close the connection to the database.
    def close
      super
      @read_pool = nil
      @secondary_pools.each do |pool|
        pool.close
      end
    end

    # If a ConnectionFailure is raised, this method will be called
    # to close the connection and reset connection values.
    # TODO: what's the point of this method?
    def reset_connection
      super
      @secondaries     = []
      @secondary_pools = []
      @arbiters        = []
      @nodes_tried  = []
      @nodes_to_try = []
    end

    private

    def check_is_master(node)
      begin
        host, port = *node
        socket = TCPSocket.new(host, port)
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

        config = self['admin'].command({:ismaster => 1}, :sock => socket)

        check_set_name(config, socket)
      rescue OperationFailure, SocketError, SystemCallError, IOError => ex
        close unless connected?
      ensure
        @nodes_tried << node
        if config
          nodes = []
          nodes += config['hosts'] if config['hosts']
          nodes += config['arbiters'] if config['arbiters']
          nodes += config['passives'] if config['passives']
          update_node_list(nodes)

          if config['msg'] && @logger
            @logger.warn("MONGODB #{config['msg']}")
          end
        end

        socket.close if socket
      end

      config
    end

    # Primary, when connecting to a replica can, can only be a true primary node.
    # (And not a slave, which is possible when connecting with the standard
    # Connection class.
    def is_primary?(config)
      config && (config['ismaster'] == 1 || config['ismaster'] == true)
    end

    # Pick a node randomly from the set of possible secondaries.
    def pick_secondary_for_read
      if (size = @secondary_pools.size) > 0
        @read_pool = @secondary_pools[rand(size)]
      end
    end

    # Make sure that we're connected to the expected replica set.
    def check_set_name(config, socket)
      if @replica_set
        config = self['admin'].command({:replSetGetStatus => 1},
                   :sock => socket, :check_response => false)

        if !Mongo::Support.ok?(config)
          raise ReplicaSetConnectionError, config['errmsg']
        elsif config['set'] != @replica_set
          raise ReplicaSetConnectionError,
            "Attempting to connect to replica set '#{config['set']}' but expected '#{@replica_set}'"
        end
      end
    end

    # Determines what kind of node we have and caches its host
    # and port so that users can easily connect manually.
    def set_auxillary(node, config)
      if config
        if config['secondary']
          host, port = *node
          @secondaries << node unless @secondaries.include?(node)
          @secondary_pools << Pool.new(self, host, port, :size => @pool_size, :timeout => @timeout)
        elsif config['arbiterOnly']
          @arbiters << node unless @arbiters.include?(node)
        end
      end
    end

    # Update the list of known nodes. Only applies to replica sets,
    # where the response to the ismaster command will return a list
    # of known hosts.
    #
    # @param hosts [Array] a list of hosts, specified as string-encoded
    #   host-port values. Example: ["myserver-1.org:27017", "myserver-1.org:27017"]
    #
    # @return [Array] the updated list of nodes
    def update_node_list(hosts)
      new_nodes = hosts.map do |host|
        if !host.respond_to?(:split)
          warn "Could not parse host #{host.inspect}."
          next
        end

        host, port = host.split(':')
        [host, port ? port.to_i : Connection::DEFAULT_PORT]
      end

      # Replace the list of seed nodes with the canonical list.
      @nodes = new_nodes.clone

      @nodes_to_try = new_nodes - @nodes_tried
    end

  end
end
