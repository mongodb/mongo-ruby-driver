module Mongo

  # Instantiates and manages connections to a MongoDB replica set.
  class MongoReplicaSetClient < MongoClient
    include ReadPreference
    include ThreadLocalVariableManager

    REPL_SET_OPTS = [
      :refresh_mode,
      :refresh_interval,
      :read_secondary,
      :rs_name,
      :name
    ]

    attr_reader :replica_set_name,
                :seeds,
                :refresh_interval,
                :refresh_mode,
                :refresh_version,
                :manager

    # Create a connection to a MongoDB replica set.
    #
    # If no args are provided, it will check <code>ENV["MONGODB_URI"]</code>.
    #
    # Once connected to a replica set, you can find out which nodes are primary, secondary, and
    # arbiters with the corresponding accessors: MongoClient#primary, MongoClient#secondaries, and
    # MongoClient#arbiters. This is useful if your application needs to connect manually to nodes other
    # than the primary.
    #
    # @overload initialize(seeds=ENV["MONGODB_URI"], opts={})
    #   @param [Array<String>, Array<Array(String, Integer)>] seeds
    #
    #   @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #     should be acknowledged
    #   @option opts [Boolean] :j (false) Set journal acknowledgement
    #   @option opts [Integer] :wtimeout (nil) Set acknowledgement timeout
    #   @option opts [Boolean] :fsync (false) Set fsync acknowledgement.
    #
    #     Notes about write concern options:
    #       Write concern options are propagated to objects instantiated from this MongoReplicaSetClient.
    #       These defaults can be overridden upon instantiation of any object by explicitly setting an options hash
    #       on initialization.
    #   @option opts [:primary, :primary_preferred, :secondary, :secondary_preferred, :nearest] :read (:primary)
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
    #   MongoReplicaSetClient.new(['localhost:30000', 'localhost:30001'])
    #
    # @example Connect to a replica set providing two seed nodes and ensuring a connection to the replica set named 'prod':
    #   MongoReplicaSetClient.new(['localhost:30000', 'localhost:30001'], :name => 'prod')
    #
    # @example Connect to a replica set providing two seed nodes and allowing reads from a secondary node:
    #   MongoReplicaSetClient.new(['localhost:30000', 'localhost:30001'], :read => :secondary)
    #
    # @see http://api.mongodb.org/ruby/current/file.REPLICA_SETS.html Replica sets in Ruby
    #
    # @raise [MongoArgumentError] This is raised for usage errors.
    #
    # @raise [ConnectionFailure] This is raised for the various connection failures.
    def initialize(*args)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      nodes = args.shift || []

      raise MongoArgumentError, "Too many arguments" unless args.empty?

      # This is temporary until support for the old format is dropped
      @seeds = nodes.collect do |node|
        if node.is_a?(Array)
          warn "Initiating a MongoReplicaSetClient with seeds passed as individual [host, port] array arguments is deprecated."
          warn "Please specify hosts as an array of 'host:port' strings; the old format will be removed in v2.0"
          node
        elsif node.is_a?(String)
          Support.normalize_seeds(node)
        else
          raise MongoArgumentError "Bad seed format!"
        end
      end

      if @seeds.empty? && ENV.has_key?('MONGODB_URI')
        parser = URIParser.new ENV['MONGODB_URI']
        if parser.direct?
          raise MongoArgumentError,
            "ENV['MONGODB_URI'] implies a direct connection."
        end
        opts = parser.connection_options.merge! opts
        @seeds = parser.nodes
      end

      if @seeds.length.zero?
        raise MongoArgumentError, "A MongoReplicaSetClient requires at least one seed node."
      end

      @seeds.freeze

      # Refresh
      @last_refresh = Time.now
      @refresh_version = 0

      # No connection manager by default.
      @manager = nil

      # Lock for request ids.
      @id_lock = Mutex.new

      @pool_mutex = Mutex.new
      @connected = false

      @safe_mutex_lock = Mutex.new
      @safe_mutexes = Hash.new {|hash, key| hash[key] = Mutex.new}

      @connect_mutex = Mutex.new
      @refresh_mutex = Mutex.new

      @mongos = false

      check_opts(opts)
      setup(opts.dup)
    end

    def valid_opts
      super + REPL_SET_OPTS - CLIENT_ONLY_OPTS
    end

    def inspect
      "<Mongo::MongoReplicaSetClient:0x#{self.object_id.to_s(16)} @seeds=#{@seeds.inspect} " +
        "@connected=#{@connected}>"
    end

    # Initiate a connection to the replica set.
    def connect
      log(:info, "Connecting...")

      # Prevent recursive connection attempts from the same thread.
      # This is done rather than using a Monitor to prevent potentially recursing
      # infinitely while attempting to connect and continually failing. Instead, fail fast.
      raise ConnectionFailure, "Failed to get node data." if thread_local[:locks][:connecting] == true

      @connect_mutex.synchronize do
        return if @connected
        begin
          thread_local[:locks][:connecting] = true
          if @manager
            ensure_manager
            @manager.refresh! @seeds
          else
            @manager = PoolManager.new(self, @seeds)
            ensure_manager
            @manager.connect
          end
        ensure
          thread_local[:locks][:connecting] = false
        end
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
    # MongoReplicaSetClient#hard_refresh!
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
      ensure_manager
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
      @manager.refresh! @seeds

      @refresh_version += 1
      return true
    end

    def connected?
      @connected && !@manager.pools.empty?
    end

    # @deprecated
    def connecting?
      warn "MongoReplicaSetClient#connecting? is deprecated and will be removed in v2.0."
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
      warn "MongoReplicaSetClient#nodes is DEPRECATED and will be removed in v2.0. " +
        "Please use MongoReplicaSetClient#seeds instead."
      @seeds
    end

    # Determine whether we're reading from a primary node. If false,
    # this connection connects to a secondary node and @read_secondaries is true.
    #
    # @return [Boolean]
    def read_primary?
      read_pool == primary_pool
    end
    alias :primary? :read_primary?

    # Close the connection to the database.
    def close(opts={})
      if opts[:soft]
        @manager.close(:soft => true) if @manager
      else
        @manager.close if @manager
      end

      # Clear the reference to this object.
      thread_local[:managers].delete(self)
      unpin_pool

      @connected = false
    end

    # If a ConnectionFailure is raised, this method will be called
    # to close the connection and reset connection values.
    # @deprecated
    def reset_connection
      close
      warn "MongoReplicaSetClient#reset_connection is now deprecated and will be removed in v2.0. " +
        "Use MongoReplicaSetClient#close instead."
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
        return socket
      else
        @connected = false
        raise ConnectionFailure.new("Could not checkout a socket.")
      end
    end

    def checkout_reader(read_pref={})
      checkout do
        pool = read_pool(read_pref)
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
        socket.checkin
      end
      sync_refresh
    end

    def ensure_manager
      thread_local[:managers][self] = @manager
    end

    def pinned_pool
      thread_local[:pinned_pools][@manager.object_id] if @manager
    end

    def pin_pool(pool, read_preference)
      if @manager
        thread_local[:pinned_pools][@manager.object_id] = {
          :pool => pool,
          :read_preference => read_preference
        }
      end
    end

    def unpin_pool
      thread_local[:pinned_pools].delete @manager.object_id if @manager
    end

    def get_socket_from_pool(pool)
      begin
        pool.checkout if pool
      rescue ConnectionFailure
        nil
      end
    end

    def local_manager
      thread_local[:managers][self]
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
      return local_manager.max_bson_size if local_manager
      DEFAULT_MAX_BSON_SIZE
    end

    def max_message_size
      return local_manager.max_message_size if local_manager
      max_bson_size * MESSAGE_SIZE_FACTOR
    end

    private

    # Parse option hash
    def setup(opts)
      # Refresh
      @refresh_mode = opts.delete(:refresh_mode) || false
      @refresh_interval = opts.delete(:refresh_interval) || 90

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

      if opts[:read_secondary]
        warn ":read_secondary options has now been deprecated and will " +
          "be removed in driver v2.0. Use the :read option instead."
        @read_secondary = opts.delete(:read_secondary) || false
      end

      # Replica set name
      if opts[:rs_name]
        warn ":rs_name option has been deprecated and will be removed in v2.0. " +
          "Please use :name instead."
        @replica_set_name = opts.delete(:rs_name)
      else
        @replica_set_name = opts.delete(:name)
      end

      super opts
    end

    def sync_refresh
      if @refresh_mode == :sync &&
        ((Time.now - @last_refresh) > @refresh_interval)

        @last_refresh = Time.now

        if @refresh_mutex.try_lock
          begin
            refresh
          ensure
            @refresh_mutex.unlock
          end
        end
      end
    end
  end
end
