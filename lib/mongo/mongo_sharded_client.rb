module Mongo

  # Instantiates and manages connections to a MongoDB sharded cluster for high availability.
  class MongoShardedClient < MongoReplicaSetClient
    include ThreadLocalVariableManager

    SHARDED_CLUSTER_OPTS = [:refresh_mode, :refresh_interval, :tag_sets, :read]

    attr_reader :seeds, :refresh_interval, :refresh_mode,
                :refresh_version, :manager

    def initialize(*args)
      opts = args.last.is_a?(Hash) ? args.pop : {}

      nodes = args.flatten

      if nodes.empty? and ENV.has_key?('MONGODB_URI')
        parser = URIParser.new ENV['MONGODB_URI']
        opts = parser.connection_options.merge! opts
        nodes = parser.node_strings
      end

      unless nodes.length > 0
        raise MongoArgumentError, "A MongoShardedClient requires at least one seed node."
      end

      @seeds = nodes.map do |host_port|
        Support.normalize_seeds(host_port)
      end

      # TODO: add a method for replacing this list of node.
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

      @mongos        = true

      check_opts(opts)
      setup(opts)
    end

    def valid_opts
      GENERIC_OPTS + SHARDED_CLUSTER_OPTS + READ_PREFERENCE_OPTS + WRITE_CONCERN_OPTS
    end

    def inspect
      "<Mongo::MongoShardedClient:0x#{self.object_id.to_s(16)} @seeds=#{@seeds.inspect} " +
          "@connected=#{@connected}>"
    end

    # Initiate a connection to the sharded cluster.
    def connect(force = !@connected)
      return unless force
      log(:info, "Connecting...")

      # Prevent recursive connection attempts from the same thread.
      # This is done rather than using a Monitor to prevent potentially recursing
      # infinitely while attempting to connect and continually failing. Instead, fail fast.
      raise ConnectionFailure, "Failed to get node data." if thread_local[:locks][:connecting]

      @connect_mutex.synchronize do
        begin
          thread_local[:locks][:connecting] = true
          if @manager
            thread_local[:managers][self] = @manager
            @manager.refresh! @seeds
          else
            @manager = ShardingPoolManager.new(self, @seeds)
            ensure_manager
            @manager.connect
          end
        ensure
          thread_local[:locks][:connecting] = false
        end

        @refresh_version += 1
        @last_refresh = Time.now
        @connected = true
      end
    end

    # Force a hard refresh of this connection's view
    # of the sharded cluster.
    #
    # @return [Boolean] +true+ if hard refresh
    #   occurred. +false+ is returned when unable
    #   to get the refresh lock.
    def hard_refresh!
      log(:info, "Initiating hard refresh...")
      connect(true)
      return true
    end

    def connected?
      @connected && @manager.primary_pool
    end

    # Returns +true+ if it's okay to read from a secondary node.
    # Since this is a sharded cluster, this must always be false.
    #
    # This method exist primarily so that Cursor objects will
    # generate query messages with a slaveOkay value of +true+.
    #
    # @return [Boolean] +true+
    def slave_ok?
      false
    end

    def checkout(&block)
      tries = 0
      begin
        super(&block)
      rescue ConnectionFailure
        tries +=1
        tries < 2 ? retry : raise
      end
    end

    # Initialize a connection to MongoDB using the MongoDB URI spec.
    #
    # @param uri [ String ]  string of the format:
    #   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/database]
    #
    # @param opts [ Hash ] Any of the options available for MongoShardedClient.new
    #
    # @return [ Mongo::MongoShardedClient ] The sharded client.
    def self.from_uri(uri, options = {})
      uri ||= ENV['MONGODB_URI']
      URIParser.new(uri).connection(options, false, true)
    end
  end
end
