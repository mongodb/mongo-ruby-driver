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

require 'sync'

module Mongo

  # Instantiates and manages connections to a MongoDB replica set.
  class ReplSetConnection < Connection
    attr_reader :nodes, :secondaries, :arbiters, :secondary_pools,
      :replica_set_name, :read_pool, :seeds, :tags_to_pools, :refresh_interval

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
    # @option opts [Boolean] :auto_refresh (false) Set this to true to enable a background thread that
    #   periodically updates the state of the connection. If, for example, you initially connect while a secondary
    #   is down, :auto_refresh will reconnect to that secondary behind the scenes to
    #   prevent you from having to reconnect manually.
    # @option opts [Integer] :refresh_interval (90) If :auto_refresh is enabled, this is the number of seconds
    #   that the background thread will sleep between calls to check the replica set's state.
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
      extend Sync_m

      if args.last.is_a?(Hash)
        opts = args.pop
      else
        opts = {}
      end

      unless args.length > 0
        raise MongoArgumentError, "A ReplSetConnection requires at least one seed node."
      end

      # The list of seed nodes
      @seeds = args

      # TODO: get rid of this
      @nodes = @seeds.dup

      # The members of the replica set, stored as instances of Mongo::Node.
      @members = []

      # Connection pool for primary node
      @primary      = nil
      @primary_pool = nil

      # Connection pools for each secondary node
      @secondaries = []
      @secondary_pools = []

      # The secondary pool to which we'll be sending reads.
      # This may be identical to the primary pool.
      @read_pool = nil

      # A list of arbiter addresses (for client information only)
      @arbiters = []

      # Refresh
      @auto_refresh = opts.fetch(:auto_refresh, false)
      @refresh_interval = opts[:refresh_interval] || 90

      # Are we allowing reads from secondaries?
      if opts[:read_secondary]
        warn ":read_secondary options has now been deprecated and will " +
          "be removed in driver v2.0. Use the :read option instead."
        @read_secondary = opts.fetch(:read_secondary, false)
        @read = :secondary
      else
        @read = opts.fetch(:read, :primary)
      end

      @connected = false

      # Store the refresher thread
      @refresh_thread = nil

      # Maps
      @sockets_to_pools = {}
      @tags_to_pools = {}

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

    def inspect
      "<Mongo::ReplSetConnection:0x#{self.object_id.to_s(16)} @seeds=#{@seeds} " +
        "@connected=#{@connected}>"
    end

    # Initiate a connection to the replica set.
    def connect
      sync_synchronize(:EX) do
        return if @connected
        manager = PoolManager.new(self, @seeds)
        manager.connect

        update_config(manager)
        initiate_auto_refresh

        if @primary.nil? #TODO: in v2.0, we'll let this be optional and do a lazy connect.
          raise ConnectionFailure, "Failed to connect to primary node."
        else
          @connected = true
        end
      end
    end

    # Note: this method must be called from within
    # an exclusive lock.
    def update_config(manager)
      @arbiters = manager.arbiters.nil? ? [] : manager.arbiters.dup
      @primary = manager.primary.nil? ? nil : manager.primary.dup
      @secondaries = manager.secondaries.dup
      @hosts = manager.hosts.dup

      @primary_pool = manager.primary_pool
      @read_pool    = manager.read_pool
      @secondary_pools = manager.secondary_pools
      @tags_to_pools   = manager.tags_to_pools
      @seeds = manager.seeds
      @manager = manager
      @nodes = manager.nodes
      @max_bson_size = manager.max_bson_size
    end

    # If ismaster doesn't match our current view
    # then create a new PoolManager, passing in our
    # existing view. It should be able to do the diff.
    # Then take out the connection lock and replace
    # our current values.
    def refresh
      return if !connected?

      if !Thread.current[:background]
        Thread.current[:background] = PoolManager.new(self, @seeds)
      end

      background_manager = Thread.current[:background]
      if background_manager.update_required?(@hosts)
        sync_synchronize(:EX) do
          background_manager.connect
          update_config(background_manager)
        end
      end
    end

    def connected?
      sync_synchronize(:SH) do
        @connected
      end
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

    def nodes
      warn "DEPRECATED"
      @seeds
    end

    # Determine whether we're reading from a primary node. If false,
    # this connection connects to a secondary node and @read_secondaries is true.
    #
    # @return [Boolean]
    def read_primary?
      sync_synchronize(:SH) do
        @read_pool == @primary_pool
      end
    end
    alias :primary? :read_primary?

    def read_preference
      @read
    end

    # Close the connection to the database.
    # TODO: we should get an exclusive lock here.
    def close
      @connected = false

      super

      if @refresh_thread
        @refresh_thread.kill
        @refresh_thread = nil
      end

      if @nodes
        @nodes.each do |member|
          member.disconnect
        end
      end

      @nodes = []
      @read_pool = nil

      if @secondary_pools
        @secondary_pools.each do |pool|
          pool.close
        end
      end

      @secondaries     = []
      @secondary_pools = []
      @arbiters        = []
      @tags_to_pools.clear
      @sockets_to_pools.clear
    end

    # If a ConnectionFailure is raised, this method will be called
    # to close the connection and reset connection values.
    # @deprecated
    def reset_connection
      close
      warn "ReplSetConnection#reset_connection is now deprecated. " +
        "Use ReplSetConnection#close instead."
    end

    # Returns +true+ if it's okay to read from a secondary node.
    # Since this is a replica set, this must always be true.
    #
    # This method exist primarily so that Cursor objects will
    # generate query messages with a slaveOkay value of +true+.
    #
    # @return [Boolean] +true+
    def slave_ok?
      true
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

    def initiate_auto_refresh
      return unless @auto_refresh
      return if @refresh_thread && @refresh_thread.alive?
      @refresh_thread = Thread.new do
        while true do
          sleep(@refresh_interval)
          refresh
        end
      end
    end

    # Checkout a socket for reading (i.e., a secondary node).
    # Note that @read_pool might point to the primary pool
    # if no read pool has been defined.
    def checkout_reader
      connect unless connected?

      sync_synchronize(:SH) do
        socket = @read_pool.checkout
        @sockets_to_pools[socket] = @read_pool
        socket
      end
    end

    # Checkout a socket connected to a node with one of
    # the provided tags. If no such node exists, raise
    # an exception.
    def checkout_tagged(tags)
      sync_synchronize(:SH) do
        tags.each do |k, v|
          pool = @tags_to_pools[{k.to_s => v}]
          if pool
            socket = pool.checkout
            @sockets_to_pools[socket] = pool
            return socket
          end
        end
      end

      raise NodeWithTagsNotFound,
        "Could not find a connection tagged with #{tags}."
    end

    # Checkout a socket for writing (i.e., a primary node).
    def checkout_writer
      connect unless connected?

      sync_synchronize(:SH) do
        if @primary_pool
          socket = @primary_pool.checkout
          @sockets_to_pools[socket] = @primary_pool
          socket
        end
      end
    end

    # Checkin a socket used for reading.
    def checkin_reader(socket)
      warn "ReplSetConnection#checkin_writer is not deprecated and will be remove " +
        "in driver v2.0. Use ReplSetConnection#checkin instead."
      checkin(socket)
    end

    # Checkin a socket used for writing.
    def checkin_writer(socket)
      warn "ReplSetConnection#checkin_writer is not deprecated and will be remove " +
        "in driver v2.0. Use ReplSetConnection#checkin instead."
      checkin(socket)
    end

    def checkin(socket)
      if pool = @sockets_to_pools[socket]
        pool.checkin(socket)
      end
    end
  end
end
