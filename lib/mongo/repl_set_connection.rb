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
    CLEANUP_INTERVAL = 300

    attr_reader :replica_set_name, :seeds, :refresh_interval, :refresh_mode,
      :refresh_version

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
    # @option opts [Float] :op_timeout (30) The number of seconds to wait for a read operation to time out.
    # @option opts [Float] :connect_timeout (30) The number of seconds to wait before timing out a
    #   connection attempt.
    # @option opts [Boolean] :ssl (false) If true, create the connection to the server using SSL.
    # @option opts [Boolean] :refresh_mode (false) Set this to :sync to periodically update the
    #   state of the connection every :refresh_interval seconds. Replica set connection failures
    #   will always trigger a complete refresh. This option is useful when you want to add new nodes
    #   or remove replica set nodes not currently in use by the driver.
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
      if args.last.is_a?(Hash)
        opts = args.pop
      else
        opts = {}
      end

      unless args.length > 0
        raise MongoArgumentError, "A ReplSetConnection requires at least one seed node."
      end

      # The original, immutable list of seed node.
      # TODO: add a method for replacing this list of node.
      @seeds = args
      @seeds.freeze

      # TODO: get rid of this
      @nodes = @seeds.dup

      # Refresh
      @refresh_mode = opts.fetch(:refresh_mode, false)
      @refresh_interval = opts[:refresh_interval] || 90
      @last_refresh = Time.now

      # No connection manager by default.
      @manager = nil
      @pool_mutex = Mutex.new

      if @refresh_mode == :async
        warn ":async refresh mode has been deprecated. Refresh
        mode will be disabled."
      elsif ![:sync, false].include?(@refresh_mode)
        raise MongoArgumentError,
          "Refresh mode must be either :sync or false."
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
      @refresh_version = 0

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
      return if @connected

      discovered_seeds = @manager ? @manager.seeds : []
      @manager = PoolManager.new(self, discovered_seeds)

      @manager.connect
      @refresh_version += 1

      if @require_primary && self.primary.nil? #TODO: in v2.0, we'll let this be optional and do a lazy connect.
        close
        raise ConnectionFailure, "Failed to connect to primary node."
      elsif self.read_pool.nil?
        close
        raise ConnectionFailure, "Failed to connect to any node."
      else
        @connected = true
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

      log(:debug, "Checking replica set connection health...")
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
      log(:info, "Initiating hard refresh...")
      discovered_seeds = @manager ? @manager.seeds : []
      background_manager = PoolManager.new(self, discovered_seeds | @seeds)
      background_manager.connect

      # TODO: make sure that connect has succeeded
      old_manager = @manager
      @manager = background_manager
      old_manager.close(:soft => true)
      @refresh_version += 1

      return true
    end

    def connected?
      @connected && (self.primary_pool || self.read_pool)
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
      self.primary_pool.host
    end

    # The replica set primary's port.
    #
    # @return [Integer]
    def port
      self.primary_pool.port
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
      self.read_pool == self.primary_pool
    end
    alias :primary? :read_primary?

    def read_preference
      @read
    end

    # Close the connection to the database.
    def close(opts={})
      if opts[:soft]
        @manager.close(:soft => true) if @manager
      else
        @manager.close if @manager
      end
      @connected = false
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
      self.primary_pool.authenticate_existing
      self.secondary_pools.each do |pool|
        pool.authenticate_existing
      end
    end

    def logout_pools(db)
      self.primary_pool.logout_existing(db)
      self.secondary_pools.each do |pool|
        pool.logout_existing(db)
      end
    end

    # Checkout a socket for reading (i.e., a secondary node).
    # Note that @read_pool might point to the primary pool
    # if no read pool has been defined.
    def checkout_reader
      if connected?
        sync_refresh
      else
        connect
      end

      begin
        socket = get_socket_from_pool(self.read_pool)

        if !socket
          connect
          socket = get_socket_from_pool(self.primary_pool)
        end
      rescue => ex
        checkin(socket) if socket
        raise ex
      end

      if socket
        socket
      else
        raise ConnectionFailure.new("Could not connect to a node for reading.")
      end
    end

    # Checkout a socket for writing (i.e., a primary node).
    def checkout_writer
      if connected?
        sync_refresh
      else
        connect
      end
      begin
        socket = get_socket_from_pool(self.primary_pool)

        if !socket
          connect
          socket = get_socket_from_pool(self.primary_pool)
        end
      rescue => ex
        checkin(socket)
        raise ex
      end

      if socket
        socket
      else
        raise ConnectionFailure.new("Could not connect to primary node.")
      end
    end

    # Checkin a socket used for reading.
    def checkin_reader(socket)
      if !((self.read_pool && self.read_pool.checkin(socket)) ||
        (self.primary_pool && self.primary_pool.checkin(socket)))
        close_socket(socket)
      end
      sync_refresh
    end

    # Checkin a socket used for writing.
    def checkin_writer(socket)
      if !self.primary_pool || !self.primary_pool.checkin(socket)
        close_socket(socket)
      end
      sync_refresh
    end

    def close_socket(socket)
      begin
        socket.close if socket
      rescue IOError
        log(:info, "Tried to close socket #{socket} but already closed.")
      end
    end

    def get_socket_from_pool(pool)
      begin
        if pool
          socket = pool.checkout
          socket
        end
      rescue ConnectionFailure => ex
        log(:info, "Failed to checkout from #{pool} with #{ex.class}; #{ex.message}")
        return nil
      end
    end

    def arbiters
      @manager.arbiters.nil? ? [] : @manager.arbiters
    end

    def primary
      @manager ? @manager.primary : nil
    end

    # Note: might want to freeze these after connecting.
    def secondaries
      @manager ? @manager.secondaries : []
    end

    def hosts
      @manager ? @manager.hosts : []
    end

    def primary_pool
      @manager ? @manager.primary_pool : nil
    end

    def read_pool
      @manager ? @manager.read_pool : nil
    end

    def secondary_pools
      @manager ? @manager.secondary_pools : []
    end

    def tag_map
      @manager ? @manager.tag_map : {}
    end

    def max_bson_size
      if @manager && @manager.max_bson_size
        @manager.max_bson_size
      else
        Mongo::DEFAULT_MAX_BSON_SIZE
      end
    end

    private

    # Generic initialization code.
    def setup(opts)
      # Default maximum BSON object size
      @max_bson_size = Mongo::DEFAULT_MAX_BSON_SIZE

      @safe_mutex_lock = Mutex.new
      @safe_mutexes = Hash.new {|hash, key| hash[key] = Mutex.new}

      # Determine whether to use SSL.
      @ssl = opts.fetch(:ssl, false)
      if @ssl
        @socket_class = Mongo::SSLSocket
      else
        @socket_class = ::TCPSocket
      end

      # Authentication objects
      @auths = opts.fetch(:auths, [])

      # Lock for request ids.
      @id_lock = Mutex.new

      # Pool size and timeout.
      @pool_size = opts[:pool_size] || 1
      if opts[:timeout]
        warn "The :timeout option has been deprecated " +
          "and will be removed in the 2.0 release. Use :pool_timeout instead."
      end
      @pool_timeout = opts[:pool_timeout] || opts[:timeout] || 5.0

      # Timeout on socket read operation.
      @op_timeout = opts[:op_timeout] || 30

      # Timeout on socket connect.
      @connect_timeout = opts[:connect_timeout] || 30

      # Mutex for synchronizing pool access
      # TODO: remove this.
      @connection_mutex = Mutex.new

      # Global safe option. This is false by default.
      @safe = opts[:safe] || false

      # Condition variable for signal and wait
      @queue = ConditionVariable.new

      @logger = opts[:logger] || nil

      # Clean up connections to dead threads.
      @last_cleanup = Time.now
      @cleanup_lock = Mutex.new

      if @logger
        write_logging_startup_message
      end

      @last_refresh = Time.now

      should_connect = opts.fetch(:connect, true)
      connect if should_connect
    end

    # Checkout a socket connected to a node with one of
    # the provided tags. If no such node exists, raise
    # an exception.
    #
    # NOTE: will be available in driver release v2.0.
    def checkout_tagged(tags)
      tags.each do |k, v|
        pool = self.tag_map[{k.to_s => v}]
        if pool
          socket = pool.checkout
          return socket
        end
      end

      raise NodeWithTagsNotFound,
        "Could not find a connection tagged with #{tags}."
    end

    def sync_refresh
      if @refresh_mode == :sync &&
        ((Time.now - @last_refresh) > @refresh_interval)
        @last_refresh = Time.now
        refresh
      end
    end
  end
end
