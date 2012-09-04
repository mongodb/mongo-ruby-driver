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

    REPL_SET_OPTS = [:read, :refresh_mode, :refresh_interval, :read_secondary, 
      :rs_name, :name, :tag_sets, :secondary_acceptable_latency_ms]

    attr_reader :replica_set_name, :seeds, :refresh_interval, :refresh_mode,
      :refresh_version, :manager, :tag_sets, :acceptable_latency

    # Create a connection to a MongoDB replica set.
    #
    # If no args are provided, it will check <code>ENV["MONGODB_URI"]</code>.
    #
    # Once connected to a replica set, you can find out which nodes are primary, secondary, and
    # arbiters with the corresponding accessors: Connection#primary, Connection#secondaries, and
    # Connection#arbiters. This is useful if your application needs to connect manually to nodes other
    # than the primary.
    #
    # @overload initialize(seeds=ENV["MONGODB_URI"], opts={})
    #   @param [Array<String>, Array<Array(String, Integer)>] seeds
    #
    #   @option opts [Boolean, Hash] :safe (false) Set the default safe-mode options
    #     propagated to DB objects instantiated off of this Connection. This
    #     default can be overridden upon instantiation of any DB by explicitly setting a :safe value
    #     on initialization.
    #   @option opts [:primary, :primary_preferred, :secondary, :secondary_preferred, :nearest] :read_preference (:primary)
    #     A "read preference" determines the candidate replica set members to which a query or command can be sent.
    #     [:primary]
    #       * Read from primary only.
    #       * Cannot be combined with tags.
    #     [:primary_preferred]
    #       * Read from primary if available, otherwise read from a secondary.
    #     [:secondary]
    #       * Read from secondary if available.
    #     [:secondary_preferred]
    #       * Read from a secondary if available, otherwise read from the primary.
    #     [:nearest]
    #       * Read from any member.
    #   @option opts [Array<Hash{ String, Symbol => Tag Value }>] :tag_sets ([])
    #     Read from replica-set members with these tags.
    #   @option opts [Integer] :secondary_acceptable_latency_ms (15) The acceptable
    #     nearest available member for a member to be considered "near".
    #   @option opts [Logger] :logger (nil) Logger instance to receive driver operation log.
    #   @option opts [Integer] :pool_size (1) The maximum number of socket connections allowed per
    #     connection pool. Note: this setting is relevant only for multi-threaded applications.
    #   @option opts [Float] :pool_timeout (5.0) When all of the connections a pool are checked out,
    #     this is the number of seconds to wait for a new connection to be released before throwing an exception.
    #     Note: this setting is relevant only for multi-threaded applications.
    #   @option opts [Float] :op_timeout (nil) The number of seconds to wait for a read operation to time out.
    #   @option opts [Float] :connect_timeout (30) The number of seconds to wait before timing out a
    #     connection attempt.
    #   @option opts [Boolean] :ssl (false) If true, create the connection to the server using SSL.
    #   @option opts [Boolean] :refresh_mode (false) Set this to :sync to periodically update the
    #     state of the connection every :refresh_interval seconds. Replica set connection failures
    #     will always trigger a complete refresh. This option is useful when you want to add new nodes
    #     or remove replica set nodes not currently in use by the driver.
    #   @option opts [Integer] :refresh_interval (90) If :refresh_mode is enabled, this is the number of seconds
    #     between calls to check the replica set's state. 
    #   @note the number of seed nodes does not have to be equal to the number of replica set members.
    #     The purpose of seed nodes is to permit the driver to find at least one replica set member even if a member is down.
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
    # @raise [MongoArgumentError] This is raised for usage errors.
    #
    # @raise [ConnectionFailure] This is raised for the various connection failures.
    def initialize(*args)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      nodes = args

      if nodes.empty? and ENV.has_key?('MONGODB_URI')
        parser = URIParser.new ENV['MONGODB_URI']
        if parser.direct?
          raise MongoArgumentError, "Mongo::ReplSetConnection.new called with no arguments, but ENV['MONGODB_URI'] implies a direct connection."
        end
        opts = parser.connection_options.merge! opts
        nodes = [parser.nodes]
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

        seeds = @manager.nil? ? @seeds : @manager.seeds
        @manager = PoolManager.new(self, seeds)

        Thread.current[:managers] ||= Hash.new
        Thread.current[:managers][self] = @manager

        @manager.connect
        @refresh_version += 1

        if @manager.pools.empty?
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
      discovered_seeds = @manager.seeds
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
      @connected && !@manager.pools.empty?
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
    #
    # This method exist primarily so that Cursor objects will
    # generate query messages with a slaveOkay value of +true+.
    #
    # @return [Boolean] +true+
    def slave_ok?
      @read != :primary
    end

    def authenticate_pools
      @manager.pools.each { |pool| pool.authenticate_existing }
    end

    def logout_pools(db)
      @manager.pools.each { |pool| pool.logout_existing(db) }
    end

    # Generic socket checkout
    # Takes a block that returns a socket from pool
    def checkout
      ensure_manager

      connected? ? sync_refresh : connect

      begin
        socket = yield
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
      socket
    end
    
    def checkout_reader(mode=@read, tag_sets=@tag_sets, acceptable_latency=@acceptable_latency)
      checkout do
        pool = read_pool(mode, tag_sets, acceptable_latency)
        get_socket_from_pool(pool)
      end
    end

    # Checkout a socket for writing (i.e., a primary node).
    def checkout_writer
      checkout do
        get_socket_from_pool(primary_pool)
      end
    end

    # Checkin a socket used for reading.
    def checkin(socket)
      if socket && socket.pool
        socket.pool.checkin(socket)
      end
      sync_refresh
    end

    def ensure_manager
      Thread.current[:managers] ||= Hash.new

      if Thread.current[:managers][self] != @manager
        Thread.current[:managers][self] = @manager
      end
    end

    def pin_pool(pool)
      @manager.pinned_pools[Thread.current] = pool if @manager
    end

    def unpin_pool(pool)
      @manager.pinned_pools[Thread.current] = nil if @manager
    end

    def get_socket_from_pool(pool)
      begin
        pool.checkout if pool
      rescue ConnectionFailure
        nil
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

    def read_pool(mode=@read, tags=@tag_sets, acceptable_latency=@acceptable_latency)
      local_manager ? local_manager.read_pool(mode, tags, acceptable_latency) : nil
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

      # Determine read preference
      if opts[:read_secondary]
        warn ":read_secondary options has now been deprecated and will " +
          "be removed in driver v2.0. Use the :read option instead."
        @read_secondary = opts.fetch(:read_secondary, false)
        @read = :secondary_preferred
      else
        @read = opts.fetch(:read, :primary)
        Mongo::Support.validate_read_preference(@read)
      end

      @tag_sets = opts.fetch(:tag_sets, [])
      @acceptable_latency = opts.fetch(:secondary_acceptable_latency_ms, 15)

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
