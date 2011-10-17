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
    attr_reader :secondaries, :arbiters, :secondary_pools,
      :replica_set_name, :read_pool, :seeds, :tag_map,
      :refresh_interval, :refresh_mode

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
    # @option options [:primary, :secondary] :read (:primary) The default read preference for Mongo::DB
    #   objects created from this connection object. If +:secondary+ is chosen, reads will be sent
    #   to one of the closest available secondary nodes. If a secondary node cannot be located, the
    #   read will be sent to the primary.
    # @option options [Logger] :logger (nil) Logger instance to receive driver operation log.
    # @option options [Integer] :pool_size (1) The maximum number of socket connections allowed per
    #   connection pool. Note: this setting is relevant only for multi-threaded applications.
    # @option options [Float] :pool_timeout (5.0) When all of the connections a pool are checked out,
    #   this is the number of seconds to wait for a new connection to be released before throwing an exception.
    #   Note: this setting is relevant only for multi-threaded applications.
    # @option opts [Float] :op_timeout (nil) The number of seconds to wait for a read operation to time out.
    #   Disabled by default.
    # @option opts [Float] :connect_timeout (nil) The number of seconds to wait before timing out a
    #   connection attempt.
    # @option opts [Boolean] :ssl (false) If true, create the connection to the server using SSL.
    # @option opts [Boolean] :refresh_mode (:sync) Set this to :async to enable a background thread that
    #   periodically updates the state of the connection. If, for example, you initially connect while a secondary
    #   is down, this will reconnect to that secondary behind the scenes to
    #   prevent you from having to reconnect manually. If set to :sync, refresh will happen
    #   synchronously. If +false+, no automatic refresh will occur unless there's a connection failure.
    # @option opts [Integer] :refresh_interval (90) If :refresh_mode is enabled, this is the number of seconds
    #   between calls to check the replica set's state.
    # @option opts [Boolean] :require_primary (true) If true, require a primary node for the connection
    #   to succeed. Otherwise, connection will succeed as long as there's at least one secondary node.
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
      @refresh_mode = opts.fetch(:refresh_mode, :sync)
      @refresh_interval = opts[:refresh_interval] || 90
      @last_refresh = Time.now

      if ![:sync, :async, false].include?(@refresh_mode)
        raise MongoArgumentError,
          "Refresh mode must be one of :sync, :async, or false."
      end

      # Are we allowing reads from secondaries?
      if opts[:read_secondary]
        warn ":read_secondary options has now been deprecated and will " +
          "be removed in driver v2.0. Use the :read option instead."
        @read_secondary = opts.fetch(:read_secondary, false)
        @read = :secondary
      else
        @read = opts.fetch(:read, :primary)
        Mongo::Support.validate_read_preference(@read)
      end

      @connected = false

      # Store the refresher thread
      @refresh_thread = nil

      # Maps
      @sockets_to_pools = {}
      @tag_map = nil

      # Replica set name
      if opts[:rs_name]
        warn ":rs_name option has been deprecated and will be removed in v2.0. " +
          "Please use :name instead."
        @replica_set_name = opts[:rs_name]
      else
        @replica_set_name = opts[:name]
      end

      # Require a primary node to connect?
      @require_primary = opts.fetch(:require_primary, true)

      setup(opts)
    end

    def inspect
      "<Mongo::ReplSetConnection:0x#{self.object_id.to_s(16)} @seeds=#{@seeds.inspect} " +
        "@connected=#{@connected}>"
    end

    # Initiate a connection to the replica set.
    def connect
      log(:info, "Connecting...")
      sync_synchronize(:EX) do
        return if @connected
        manager = PoolManager.new(self, @seeds)
        manager.connect

        update_config(manager)
        initiate_refresh_mode

        if @require_primary && @primary.nil? #TODO: in v2.0, we'll let this be optional and do a lazy connect.
          raise ConnectionFailure, "Failed to connect to primary node."
        elsif !@read_pool
          raise ConnectionFailure, "Failed to connect to any node."
        else
          @connected = true
        end
      end
    end

    # Determine whether a replica set refresh is
    # required. If so, run a hard refresh. You can
    # force a hard refresh by running
    # ReplSetConnection#hard_refresh!
    #
    # @return [Boolean] +true+ unless a hard refresh
    #   is run and the refresh lock can't be acquired.
    def refresh(opts={})
      if !connected?
        log(:info, "Trying to check replica set health but not " +
          "connected...")
        return hard_refresh!
      end

      log(:info, "Checking replica set connection health...")
      @manager.check_connection_health

      if @manager.refresh_required?
        return hard_refresh!
      end

      return true
    end

    # Force a hard refresh of this connection's view
    # of the replica set.
    #
    # @return [Boolean] +true+ if hard refresh
    #   occurred. +false+ is returned when unable
    #   to get the refresh lock.
    def hard_refresh!
      return false if sync_exclusive?

      log(:info, "Initiating hard refresh...")
      @background_manager = PoolManager.new(self, @seeds)
      @background_manager.connect

      sync_synchronize(:EX) do
        @manager.close
        update_config(@background_manager)
      end

      initiate_refresh_mode

      return true
    end

    def connected?
      !@primary_pool.nil? || !@read_pool.nil?
    end

    # @deprecated
    def connecting?
      warn "ReplSetConnection#connecting? is deprecated and will be removed in v2.0."
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
      warn "ReplSetConnection#nodes is DEPRECATED and will be removed in v2.0. " +
        "Please use ReplSetConnection#seeds instead."
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
    def close
      sync_synchronize(:EX) do
        @connected = false
        super

        if @refresh_thread
          @refresh_thread.kill
          @refresh_thread = nil
        end

        @read_pool = nil

        if @secondary_pools
          @secondary_pools.each do |pool|
            pool.close
          end
        end

        @secondaries      = []
        @secondary_pools  = []
        @arbiters         = []
        @tag_map = nil
        @sockets_to_pools.clear
      end
    end

    # If a ConnectionFailure is raised, this method will be called
    # to close the connection and reset connection values.
    # @deprecated
    def reset_connection
      close
      warn "ReplSetConnection#reset_connection is now deprecated and will be removed in v2.0. " +
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

    # Checkout a socket for reading (i.e., a secondary node).
    # Note that @read_pool might point to the primary pool
    # if no read pool has been defined.
    def checkout_reader
      connect unless connected?
      socket = get_socket_from_pool(@read_pool)

      if !socket
        refresh
        socket = get_socket_from_pool(@primary_pool)
      end

      if socket
        socket
      else
        raise ConnectionFailure.new("Could not connect to a node for reading.")
      end
    end

    # Checkout a socket for writing (i.e., a primary node).
    def checkout_writer
      connect unless connected?
      socket = get_socket_from_pool(@primary_pool)

      if !socket
        refresh
        socket = get_socket_from_pool(@primary_pool)
      end

      if socket
        socket
      else
        raise ConnectionFailure.new("Could not connect to primary node.")
      end
    end

    def checkin(socket)
      sync_synchronize(:SH) do
        if pool = @sockets_to_pools[socket]
          pool.checkin(socket)
        elsif socket
          begin
            socket.close
          rescue IOError
            log(:info, "Tried to close socket #{socket} but already closed.")
          end
        end
      end

      # Refresh synchronously every @refresh_interval seconds
      # if synchronous refresh mode is enabled.
      if @refresh_mode == :sync &&
        ((Time.now - @last_refresh) > @refresh_interval)
        refresh
        @last_refresh = Time.now
      end
    end

    def get_socket_from_pool(pool)
      begin
        sync_synchronize(:SH) do
          if pool
            socket = pool.checkout
            @sockets_to_pools[socket] = pool
            socket
          end
        end

      rescue ConnectionFailure => ex
        log(:info, "Failed to checkout from #{pool} with #{ex.class}; #{ex.message}")
        return nil
      end
    end

    private

    # Given a pool manager, update this connection's
    # view of the replica set.
    #
    # This method must be called within
    # an exclusive lock.
    def update_config(manager)
      @arbiters = manager.arbiters.nil? ? [] : manager.arbiters.dup
      @primary = manager.primary.nil? ? nil : manager.primary.dup
      @secondaries = manager.secondaries.dup
      @hosts = manager.hosts.dup

      @primary_pool = manager.primary_pool
      @read_pool    = manager.read_pool
      @secondary_pools = manager.secondary_pools
      @tag_map = manager.tag_map
      @seeds = manager.seeds
      @manager = manager
      @nodes = manager.nodes
      @max_bson_size = manager.max_bson_size
      @sockets_to_pools.clear
    end

    def initiate_refresh_mode
      if @refresh_mode == :async
        return if @refresh_thread && @refresh_thread.alive?
        @refresh_thread = Thread.new do
          while true do
            sleep(@refresh_interval)
            refresh
          end
        end
      end

      @last_refresh = Time.now
    end

    # Checkout a socket connected to a node with one of
    # the provided tags. If no such node exists, raise
    # an exception.
    #
    # NOTE: will be available in driver release v2.0.
    def checkout_tagged(tags)
      sync_synchronize(:SH) do
        tags.each do |k, v|
          pool = @tag_map[{k.to_s => v}]
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

    # Checkin a socket used for reading.
    def checkin_reader(socket)
      warn "ReplSetConnection#checkin_writer is deprecated and will be removed " +
        "in driver v2.0. Use ReplSetConnection#checkin instead."
      checkin(socket)
    end

    # Checkin a socket used for writing.
    def checkin_writer(socket)
      warn "ReplSetConnection#checkin_writer is deprecated and will be removed " +
        "in driver v2.0. Use ReplSetConnection#checkin instead."
      checkin(socket)
    end
  end
end
