# encoding: UTF-8

# --
# Copyright (C) 2008-2011 10gen Inc.
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

  # Instantiates and manages connections to a MongoDB replica set.
  class ReplSetConnection < Connection
    attr_reader :nodes, :secondaries, :arbiters, :read_pool, :secondary_pools,
      :replica_set_name, :members

    # Create a connection to a MongoDB replica set.
    #
    # Once connected to a replica set, you can find out which nodes are primary, secondary, and
    # arbiters with the corresponding accessors: Connection#primary, Connection#secondaries, and
    # Connection#arbiters. This is useful if your application needs to connect manually to nodes other
    # than the primary.
    #
    # @param [Array] args A list of host-port pairs to be used as seed nodes followed by a
    #   hash containing any options. See the examples below for exactly how to use the constructor.
    #
    # @option options [String] :rs_name (nil) The name of the replica set to connect to. You
    #   can use this option to verify that you're connecting to the right replica set.
    # @option options [Boolean, Hash] :safe (false) Set the default safe-mode options
    #   propogated to DB objects instantiated off of this Connection. This
    #   default can be overridden upon instantiation of any DB by explicity setting a :safe value
    #   on initialization.
    # @option options [Boolean] :read_secondary(false) If true, a random secondary node will be chosen,
    #   and all reads will be directed to that node.
    # @option options [Logger, #debug] :logger (nil) Logger instance to receive driver operation log.
    # @option options [Integer] :pool_size (1) The maximum number of socket connections allowed per
    #   connection pool. Note: this setting is relevant only for multi-threaded applications.
    # @option options [Float] :pool_timeout (5.0) When all of the connections a pool are checked out,
    #   this is the number of seconds to wait for a new connection to be released before throwing an exception.
    #   Note: this setting is relevant only for multi-threaded applications.
    # @option opts [Float] :op_timeout (nil) The number of seconds to wait for a read operation to time out.
    #   Disabled by default.
    # @option opts [Float] :connect_timeout (nil) The number of seconds to wait before timing out a
    #   connection attempt.
    #
    # @example Connect to a replica set and provide two seed nodes. Note that the number of seed nodes does
    #   not have to be equal to the number of replica set members. The purpose of seed nodes is to permit
    #   the driver to find at least one replica set member even if a member is down.
    #   ReplSetConnection.new(['localhost', 30000], ['localhost', 30001])
    #
    # @example Connect to a replica set providing two seed nodes and ensuring a connection to the replica set named 'prod':
    #   ReplSetConnection.new(['localhost', 30000], ['localhost', 30001], :rs_name => 'prod')
    #
    # @example Connect to a replica set providing two seed nodes and allowing reads from a secondary node:
    #   ReplSetConnection.new(['localhost', 30000], ['localhost', 30001], :read_secondary => true)
    #
    # @see http://api.mongodb.org/ruby/current/file.REPLICA_SETS.html Replica sets in Ruby
    #
    # @raise [ReplicaSetConnectionError] This is raised if a replica set name is specified and the
    #   driver fails to connect to a replica set with that name.
    def initialize(*args)
      if args.last.is_a?(Hash)
        opts = args.pop
      else
        opts = {}
      end

      unless args.length > 0
        raise MongoArgumentError, "A ReplSetConnection requires at least one node."
      end

      # Get the list of seed nodes
      @seeds = args

      # The members of the replica set, stored as instances of Mongo::Node.
      @members = []

      # Connection pool for primay node
      @primary      = nil
      @primary_pool = nil

      # Connection pools for each secondary node
      @secondaries = []
      @secondary_pools = []

      # The secondary pool to which we'll be sending reads.
      @read_pool = nil

      # A list of arbiter address (for client information only)
      @arbiters = []

      # Are we allowing reads from secondaries?
      @read_secondary = opts.fetch(:read_secondary, false)

      # Replica set name
      if opts[:rs_name]
        warn ":rs_name option has been deprecated and will be removed in 2.0. " +
          "Please use :name instead."
        @replica_set_name = opts[:rs_name]
      else
        @replica_set_name = opts[:name]
      end

      setup(opts)
    end

    # Use the provided seed nodes to initiate a connection
    # to the replica set.
    def connect
      connect_to_members
      initialize_pools
      pick_secondary_for_read

      if connected?
        BSON::BSON_CODER.update_max_bson_size(self)
      else
        close

        if @primary.nil?
          raise ConnectionFailure, "Failed to connect to primary node."
        else
          raise ConnectionFailure, "Failed to connect to any given member."
        end
      end
    end

    def connected?
      @primary_pool || (@read_pool && @read_secondary)
    end

    # @deprecated
    def connecting?
      false
    end

    # The replica set primary's host name.
    #
    # @return [String]
    def host
      super
    end

    # The replica set primary's port.
    #
    # @return [Integer]
    def port
      super
    end

    # Determine whether we're reading from a primary node. If false,
    # this connection connects to a secondary node and @read_secondaries is true.
    #
    # @return [Boolean]
    def read_primary?
      !@read_pool
    end
    alias :primary? :read_primary?

    # Close the connection to the database.
    def close
      super
      @members.each do |member|
        member.disconnect
      end
      @members = []
      @read_pool = nil
      @secondary_pools.each do |pool|
        pool.close
      end
      @secondaries     = []
      @secondary_pools = []
      @arbiters        = []
    end

    # If a ConnectionFailure is raised, this method will be called
    # to close the connection and reset connection values.
    # @deprecated
    def reset_connection
      close
      warn "ReplSetConnection#reset_connection is now deprecated. " +
        "Use ReplSetConnection#close instead."
    end

    # Is it okay to connect to a slave?
    #
    # @return [Boolean]
    def slave_ok?
      @read_secondary
    end

    def authenticate_pools
      super
      @secondary_pools.each do |pool|
        pool.authenticate_existing
      end
    end

    def logout_pools(db)
      super
      @secondary_pools.each do |pool|
        pool.logout_existing(db)
      end
    end

    private

    # Iterate through the list of provided seed
    # nodes until we've gotten a response from the
    # replica set we're trying to connect to.
    #
    # If we don't get a response, raise an exception.
    def get_valid_seed_node
      @seeds.each do |seed|
        node = Mongo::Node.new(self, seed)
        if node.connect && node.set_config
          return node
        end
      end

      raise ConnectionFailure, "Cannot connect to a replica set with name using seed nodes " +
        "#{@seeds.map {|s| "#{s[0]}:#{s[1]}" }.join(',')}"
    end

    # Connect to each member of the replica set
    # as reported by the given seed node, and cache
    # those connections in the @members array.
    def connect_to_members
      seed = get_valid_seed_node

      seed.node_list.each do |host|
        node = Mongo::Node.new(self, host)
        if node.connect && node.set_config
          @members << node
        end
      end
    end

    # Initialize the connection pools to the primary and secondary nodes.
    def initialize_pools
      if @members.empty?
        raise ConnectionFailure, "Failed to connect to any given member."
      end

      @arbiters = @members.first.arbiters

      @members.each do |member|
        if member.primary?
          @primary = member.host_port
          @primary_pool = Pool.new(self, member.host, member.port, :size => @pool_size, :timeout => @timeout)
        elsif member.secondary? && !@secondaries.include?(member.host_port)
          @secondaries << member.host_port
          @secondary_pools << Pool.new(self, member.host, member.port, :size => @pool_size, :timeout => @timeout)
        end
      end
    end

    # Pick a node randomly from the set of possible secondaries.
    def pick_secondary_for_read
      return unless @read_secondary
      if (size = @secondary_pools.size) > 0
        @read_pool = @secondary_pools[rand(size)]
      end
    end

    # Checkout a socket for reading (i.e., a secondary node).
    def checkout_reader
      connect unless connected?

      if @read_pool
        @read_pool.checkout
      else
        checkout_writer
      end
    end

    # Checkout a socket for writing (i.e., a primary node).
    def checkout_writer
      connect unless connected?

      @primary_pool.checkout
    end

    # Checkin a socket used for reading.
    def checkin_reader(socket)
      if @read_pool
        @read_pool.checkin(socket)
      else
        checkin_writer(socket)
      end
    end

    # Checkin a socket used for writing.
    def checkin_writer(socket)
      if @primary_pool
        @primary_pool.checkin(socket)
      end
    end
  end
end
