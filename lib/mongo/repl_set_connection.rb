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

    REPL_SET_OPTS = [:read, :refresh_mode, :refresh_interval, :require_primary,
      :read_secondary, :rs_name, :name]

    attr_reader :replica_set_name, :seeds, :refresh_interval, :refresh_mode,
      :refresh_version, :manager

    # Create a connection to a MongoDB replica set.
    #
    # If no args are provided, it will check <code>ENV["MONGODB_URI"]</code>.
    #
    # Once connected to a replica set, you can find out which nodes are primary, secondary, and
    # arbiters with the corresponding accessors: Connection#primary, Connection#secondaries, and
    # Connection#arbiters. This is useful if your application needs to connect manually to nodes other
    # than the primary.
    #
    # @param [Array] seeds "host:port" strings
    #
    # @option opts [String] :name (nil) The name of the replica set to connect to. You
    #   can use this option to verify that you're connecting to the right replica set.
    # @option opts [Boolean, Hash] :safe (false) Set the default safe-mode options
    #   propogated to DB objects instantiated off of this Connection. This
    #   default can be overridden upon instantiation of any DB by explicity setting a :safe value
    #   on initialization.
    # @option opts [:primary, :secondary] :read (:primary) The default read preference for Mongo::DB
    #   objects created from this connection object. If +:secondary+ is chosen, reads will be sent
    #   to one of the closest available secondary nodes. If a secondary node cannot be located, the
    #   read will be sent to the primary.
    # @option opts [Logger] :logger (nil) Logger instance to receive driver operation log.
    # @option opts [Integer] :pool_size (1) The maximum number of socket connections allowed per
    #   connection pool. Note: this setting is relevant only for multi-threaded applications.
    # @option opts [Float] :pool_timeout (5.0) When all of the connections a pool are checked out,
    #   this is the number of seconds to wait for a new connection to be released before throwing an exception.
    #   Note: this setting is relevant only for multi-threaded applications.
    # @option opts [Float] :op_timeout (nil) The number of seconds to wait for a read operation to time out.
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
    # Note: that the number of seed nodes does not have to be equal to the number of replica set members.
    # The purpose of seed nodes is to permit the driver to find at least one replica set member even if a member is down.
    #
    # @example Connect to a replica set and provide two seed nodes.
    #   Mongo::ReplSetConnection.new(['localhost:30000', 'localhost:30001'])
    #
    # @example Connect to a replica set providing two seed nodes and ensuring a connection to the replica set named 'prod':
    #   Mongo::ReplSetConnection.new(['localhost:30000', 'localhost:30001'], :name => 'prod')
    #
    # @example Connect to a replica set providing two seed nodes and allowing reads from a secondary node:
    #   Mongo::ReplSetConnection.new(['localhost:30000', 'localhost:30001'], :read => :secondary)
    #
    # @see http://api.mongodb.org/ruby/current/file.REPLICA_SETS.html Replica sets in Ruby
    #
    # @raise [MongoArgumentError] If called with no arguments and <code>ENV["MONGODB_URI"]</code> implies a direct connection.
    #
    # @raise [ReplicaSetConnectionError] This is raised if a replica set name is specified and the
    #   driver fails to connect to a replica set with that name.
    def initialize(*args)
      if args.last.is_a?(Hash)
        opts = args.pop
      else
        opts = {}
      end

      nodes = args

      if nodes.empty? and ENV.has_key?('MONGODB_URI')
        parser = URIParser.new ENV['MONGODB_URI'], opts
        if parser.direct?
          raise MongoArgumentError, "Mongo::ReplSetConnection.new called with no arguments, but ENV['MONGODB_URI'] implies a direct connection."
        end
        opts = parser.connection_options
        nodes = parser.nodes
      end

      unless nodes.length > 0
        raise MongoArgumentError, "A ReplSetConnection requires at least one seed node."
      end

      # This is temporary until support for the old format is dropped
      if nodes.first.last.is_a?(Integer)
        warn "Initiating a ReplSetConnection with seeds passed as individual [host, port] array arguments is deprecated."
        warn "Please specify hosts as an array of 'host:port' strings; the old format will be removed in v2.0"
        @seeds = nodes
      else
        @seeds = nodes.first.map do |host_port|
          host, port = host_port.split(":")
          [ host, port.to_i ]
        end
      end

      # TODO: add a method for replacing this list of node.
      @seeds.freeze

      # Refresh
      @last_refresh = Time.now
      @refresh_version = 0

      # No connection manager by default.
      @manager = nil
      @old_managers = []

      # Lock for request ids.
      @id_lock = Mutex.new

      @pool_mutex = Mutex.new
      @connected = false

      @safe_mutex_lock = Mutex.new
      @safe_mutexes = Hash.new {|hash, key| hash[key] = Mutex.new}

      @connect_mutex = Mutex.new
      @refresh_mutex = Mutex.new

      check_opts(opts)
      setup(opts)
    end

    def valid_opts
      GENERIC_OPTS + REPL_SET_OPTS
    end

    def inspect
      "<Mongo::ReplSetConnection:0x#{self.object_id.to_s(16)} @seeds=#{@seeds.inspect} " +
        "@connected=#{@connected}>"
    end

    # Initiate a connection to the replica set.
    def connect
      log(:info, "Connecting...")
      @connect_mutex.synchronize do
        return if @connected

        discovered_seeds = @manager ? @manager.seeds : []
        @manager = PoolManager.new(self, discovered_seeds)

        Thread.current[:managers] ||= Hash.new
        Thread.current[:managers][self] = @manager

        @manager.connect
        @refresh_version += 1

        if @require_primary && @manager.primary.nil? #TODO: in v2.0, we'll let this be optional and do a lazy connect.
          close
          raise ConnectionFailure, "Failed to connect to primary node."
        elsif @manager.read_pool.nil?
          close
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
      new_manager = PoolManager.new(self, discovered_seeds | @seeds)
      new_manager.connect

      Thread.current[:managers][self] = new_manager

      # TODO: make sure that connect has succeeded
      @old_managers << @manager
      @manager = new_manager

      @refresh_version += 1
      return true
    end

    def connected?
      @connected && (@manager.primary_pool || @manager.read_pool)
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
      @manager.primary_pool.host
    end

    # The replica set primary's port.
    #
    # @return [Integer]
    def port
      @manager.primary_pool.port
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
      @manager.read_pool == @manager.primary_pool
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

      # Clear the reference to this object.
      if Thread.current[:managers]
        Thread.current[:managers].delete(self)
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
      if primary_pool
        primary_pool.authenticate_existing
      end
      secondary_pools.each do |pool|
        pool.authenticate_existing
      end
    end

    def logout_pools(db)
      if primary_pool
        primary_pool.logout_existing(db)
      end
      secondary_pools.each do |pool|
        pool.logout_existing(db)
      end
    end

    # Generic socket checkout
    # Takes a block that returns a socket from pool
    def checkout(&block)
      if connected?
        sync_refresh
      else
        connect
      end
      
      begin
        socket = block.call
      rescue => ex
        checkin(socket) if socket
        raise ex
      end
      
      if socket
        socket
      else
        @connected = false
        raise ConnectionFailure.new("Could not checkout a socket.")
      end
    end

    # Checkout best available socket by trying primary
    # pool first and then falling back to secondary.
    def checkout_best
      checkout do
        socket = get_socket_from_pool(:primary)
        if !socket
          connect
          socket = get_socket_from_pool(:secondary)
        end
        socket
      end
    end
    
    # Checkout a socket for reading (i.e., a secondary node).
    # Note that @read_pool might point to the primary pool
    # if no read pool has been defined.
    def checkout_reader
      checkout do
        socket = get_socket_from_pool(:read)
        if !socket
          connect
          socket = get_socket_from_pool(:primary)
        end
        socket
      end
    end

    # Checkout a socket from a secondary
    # For :read_preference => :secondary_only
    def checkout_secondary
      checkout do
        get_socket_from_pool(:secondary)
      end
    end

    # Checkout a socket for writing (i.e., a primary node).
    def checkout_writer
      checkout do
        get_socket_from_pool(:primary)
      end
    end

    # Checkin a socket used for reading.
    def checkin_reader(socket)
      if socket
        socket.pool.checkin(socket)
      end
      sync_refresh
    end

    # Checkin a socket used for writing.
    def checkin_writer(socket)
      if socket
        socket.pool.checkin(socket)
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

    def ensure_manager
      Thread.current[:managers] ||= Hash.new

      if Thread.current[:managers][self] != @manager
        Thread.current[:managers][self] = @manager
      end
    end

    def get_socket_from_pool(pool_type)
      ensure_manager

      pool = case pool_type
        when :primary
          primary_pool
        when :secondary
          secondary_pool
        when :read
          read_pool
      end

      begin
        if pool
          pool.checkout
        end
      rescue ConnectionFailure => ex
        log(:info, "Failed to checkout from #{pool} with #{ex.class}; #{ex.message}")
        return nil
      end
    end

    def local_manager
      Thread.current[:managers][self] if Thread.current[:managers]
    end

    def arbiters
      local_manager.arbiters.nil? ? [] : local_manager.arbiters
    end

    def primary
      local_manager ? local_manager.primary : nil
    end

    # Note: might want to freeze these after connecting.
    def secondaries
      local_manager ? local_manager.secondaries : []
    end

    def hosts
      local_manager ? local_manager.hosts : []
    end

    def primary_pool
      local_manager ? local_manager.primary_pool : nil
    end

    def read_pool
      local_manager ? local_manager.read_pool : nil
    end

    def secondary_pool
      local_manager ? local_manager.secondary_pool : nil
    end

    def secondary_pools
      local_manager ? local_manager.secondary_pools : []
    end

    def tag_map
      local_manager ? local_manager.tag_map : {}
    end

    def max_bson_size
      if local_manager && local_manager.max_bson_size
        local_manager.max_bson_size
      else
        Mongo::DEFAULT_MAX_BSON_SIZE
      end
    end

    private

    # Parse option hash
    def setup(opts)
      # Require a primary node to connect?
      @require_primary = opts.fetch(:require_primary, true)

      # Refresh
      @refresh_mode = opts.fetch(:refresh_mode, false)
      @refresh_interval = opts.fetch(:refresh_interval, 90)

      if @refresh_mode && @refresh_interval < 60
        @refresh_interval = 60 unless ENV['TEST_MODE'] = 'TRUE'
      end

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

      # Replica set name
      if opts[:rs_name]
        warn ":rs_name option has been deprecated and will be removed in v2.0. " +
          "Please use :name instead."
        @replica_set_name = opts[:rs_name]
      else
        @replica_set_name = opts[:name]
      end

      opts[:connect_timeout] = opts[:connect_timeout] || 30

      super opts
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
    
    def prune_managers
      @old_managers.each do |manager|
        if manager != @manager
          if manager.closed?
            @old_managers.delete(manager)
          else
            manager.close(:soft => true)
          end
        end
      end
    end

    def sync_refresh
      if @refresh_mode == :sync &&
        ((Time.now - @last_refresh) > @refresh_interval)
        @last_refresh = Time.now
        
        if @refresh_mutex.try_lock
          begin
            refresh
            prune_managers
          ensure
            @refresh_mutex.unlock
          end
        end
      end
    end
  end
end
