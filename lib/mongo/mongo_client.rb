require 'set'
require 'socket'
require 'thread'

module Mongo

  # Instantiates and manages self.connections to MongoDB.
  class MongoClient
    include Mongo::Logging
    include Mongo::Networking
    include Mongo::WriteConcern

    Mutex              = ::Mutex
    ConditionVariable  = ::ConditionVariable

    DEFAULT_HOST         = 'localhost'
    DEFAULT_PORT         = 27017
    DEFAULT_DB_NAME      = 'test'
    GENERIC_OPTS         = [:ssl, :auths, :logger, :connect]
    TIMEOUT_OPTS         = [:timeout, :op_timeout, :connect_timeout]
    POOL_OPTS            = [:pool_size, :pool_timeout]
    READ_PREFERENCE_OPTS = [:read, :tag_sets, :secondary_acceptable_latency_ms]
    WRITE_CONCERN_OPTS   = [:w, :j, :fsync, :wtimeout]
    CLIENT_ONLY_OPTS     = [:slave_ok]

    mongo_thread_local_accessor :connections

    attr_reader :logger,
                :size,
                :auths,
                :primary,
                :write_concern,
                :host_to_try,
                :pool_size,
                :connect_timeout,
                :pool_timeout,
                :primary_pool,
                :socket_class,
                :op_timeout,
                :tag_sets,
                :acceptable_latency,
                :read

    # Create a connection to single MongoDB instance.
    #
    # If no args are provided, it will check <code>ENV["MONGODB_URI"]</code>.
    #
    # You may specify whether connection to slave is permitted.
    # In all cases, the default host is "localhost" and the default port is 27017.
    #
    # If you're connecting to a replica set, you'll need to use MongoReplicaSetClient.new instead.
    #
    # Once connected to a replica set, you can find out which nodes are primary, secondary, and
    # arbiters with the corresponding accessors: MongoClient#primary, MongoClient#secondaries, and
    # MongoClient#arbiters. This is useful if your application needs to connect manually to nodes other
    # than the primary.
    #
    # @param [String] host
    # @param [Integer] port specify a port number here if only one host is being specified.
    #
    # @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #   should be acknowledged
    # @option opts [Boolean] :j (false) Set journal acknowledgement
    # @option opts [Integer] :wtimeout (nil) Set replica set acknowledgement timeout
    # @option opts [Boolean] :fsync (false) Set fsync acknowledgement.
    #
    #     Notes about write concern options:
    #       Write concern options are propagated to objects instantiated from this MongoClient.
    #       These defaults can be overridden upon instantiation of any object by explicitly setting an options hash
    #       on initialization.
    # @option opts [Boolean] :slave_ok (false) Must be set to +true+ when connecting
    #   to a single, slave node.
    # @option opts [Logger, #debug] :logger (nil) A Logger instance for debugging driver ops. Note that
    #   logging negatively impacts performance; therefore, it should not be used for high-performance apps.
    # @option opts [Integer] :pool_size (1) The maximum number of socket self.connections allowed per
    #   connection pool. Note: this setting is relevant only for multi-threaded applications.
    # @option opts [Float] :timeout (5.0) When all of the self.connections a pool are checked out,
    #   this is the number of seconds to wait for a new connection to be released before throwing an exception.
    #   Note: this setting is relevant only for multi-threaded applications (which in Ruby are rare).
    # @option opts [Float] :op_timeout (nil) The number of seconds to wait for a read operation to time out.
    #   Disabled by default.
    # @option opts [Float] :connect_timeout (nil) The number of seconds to wait before timing out a
    #   connection attempt.
    # @option opts [Boolean] :ssl (false) If true, create the connection to the server using SSL.
    #
    # @example localhost, 27017 (or <code>ENV["MONGODB_URI"]</code> if available)
    #   MongoClient.new
    #
    # @example localhost, 27017
    #   MongoClient.new("localhost")
    #
    # @example localhost, 3000, max 5 self.connections, with max 5 seconds of wait time.
    #   MongoClient.new("localhost", 3000, :pool_size => 5, :timeout => 5)
    #
    # @example localhost, 3000, where this node may be a slave
    #   MongoClient.new("localhost", 3000, :slave_ok => true)
    #
    # @example Unix Domain Socket
    #   MongoClient.new("/var/run/mongodb.sock")
    #
    # @see http://api.mongodb.org/ruby/current/file.REPLICA_SETS.html Replica sets in Ruby
    #
    # @raise [ReplicaSetConnectionError] This is raised if a replica set name is specified and the
    #   driver fails to connect to a replica set with that name.
    #
    # @raise [MongoArgumentError] If called with no arguments and <code>ENV["MONGODB_URI"]</code> implies a replica set.
    #
    # @core self.connections
    def initialize(*args)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      @host, @port = parse_init(args[0], args[1], opts)

      # Lock for request ids.
      @id_lock = Mutex.new

      # Connection pool for primary node
      @primary      = nil
      @primary_pool = nil
      @mongos       = false

      # Not set for direct connection
      @tag_sets = []
      @acceptable_latency = 15

      check_opts(opts)
      setup(opts.dup)
    end

    # DEPRECATED
    #
    # Initialize a connection to a MongoDB replica set using an array of seed nodes.
    #
    # The seed nodes specified will be used on the initial connection to the replica set, but note
    # that this list of nodes will be replaced by the list of canonical nodes returned by running the
    # is_master command on the replica set.
    #
    # @param nodes [Array] An array of arrays, each of which specifies a host and port.
    # @param opts [Hash] Any of the available options that can be passed to MongoClient.new.
    #
    # @option opts [String] :rs_name (nil) The name of the replica set to connect to. An exception will be
    #   raised if unable to connect to a replica set with this name.
    # @option opts [Boolean] :read_secondary (false) When true, this connection object will pick a random slave
    #   to send reads to.
    #
    # @example
    #   Mongo::MongoClient.multi([["db1.example.com", 27017], ["db2.example.com", 27017]])
    #
    # @example This connection will read from a random secondary node.
    #   Mongo::MongoClient.multi([["db1.example.com", 27017], ["db2.example.com", 27017], ["db3.example.com", 27017]],
    #                   :read_secondary => true)
    #
    # @return [Mongo::MongoClient]
    #
    # @deprecated
    def self.multi(nodes, opts={})
      warn "MongoClient.multi is now deprecated and will be removed in v2.0. Please use MongoReplicaSetClient.new instead."

      MongoReplicaSetClient.new(nodes, opts)
    end

    # Initialize a connection to MongoDB using the MongoDB URI spec.
    #
    # Since MongoClient.new cannot be used with any <code>ENV["MONGODB_URI"]</code> that has multiple hosts (implying a replicaset), 
    # you may use this when the type of your connection varies by environment and should be determined solely from <code>ENV["MONGODB_URI"]</code>.
    #
    # @param uri [String]
    #   A string of the format mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/database]
    #
    # @param opts Any of the options available for MongoClient.new
    #
    # @return [Mongo::MongoClient, Mongo::MongoReplicaSetClient]
    def self.from_uri(uri = ENV['MONGODB_URI'], extra_opts={})
      parser = URIParser.new uri
      parser.connection(extra_opts)
    end

    def parse_init(host, port, opts)
      if host.nil? && port.nil? && ENV.has_key?('MONGODB_URI')
        parser = URIParser.new(ENV['MONGODB_URI'])
        if parser.replicaset?
          raise MongoArgumentError,
            "ENV['MONGODB_URI'] implies a replica set."
        end
        opts.merge! parser.connection_options
        [parser.host, parser.port]
      else
        [host || DEFAULT_HOST, port || DEFAULT_PORT]
      end
    end

    # The host name used for this connection.
    #
    # @return [String]
    def host
      @primary_pool.host
    end

    # The port used for this connection.
    #
    # @return [Integer]
    def port
      @primary_pool.port
    end

    def host_port
      [@host, @port]
    end

    # Fsync, then lock the mongod process against writes. Use this to get
    # the datafiles in a state safe for snapshotting, backing up, etc.
    #
    # @return [BSON::OrderedHash] the command response
    def lock!
      cmd = BSON::OrderedHash.new
      cmd[:fsync] = 1
      cmd[:lock]  = true
      self['admin'].command(cmd)
    end

    # Is this database locked against writes?
    #
    # @return [Boolean]
    def locked?
      [1, true].include? self['admin']['$cmd.sys.inprog'].find_one['fsyncLock']
    end

    # Unlock a previously fsync-locked mongod process.
    #
    # @return [BSON::OrderedHash] command response
    def unlock!
      self['admin']['$cmd.sys.unlock'].find_one
    end

    # Apply each of the saved database authentications.
    #
    # @return [Boolean] returns true if authentications exist and succeeds, false
    #   if none exists.
    #
    # @raise [AuthenticationError] raises an exception if any one
    #   authentication fails.
    def apply_saved_authentication(opts={})
      return false if @auths.empty?
      @auths.each do |auth|
        self[auth[:db_name]].issue_authentication(auth[:username], auth[:password], false,
          :socket => opts[:socket])
      end
      true
    end

    # Save an authentication to this connection. When connecting,
    # the connection will attempt to re-authenticate on every db
    # specified in the list of auths. This method is called automatically
    # by DB#authenticate.
    #
    # Note: this method will not actually issue an authentication command. To do that,
    # either run MongoClient#apply_saved_authentication or DB#authenticate.
    #
    # @param [String] db_name
    # @param [String] username
    # @param [String] password
    #
    # @return [Hash] a hash representing the authentication just added.
    def add_auth(db_name, username, password)
      if @auths.any? {|a| a[:db_name] == db_name}
        raise MongoArgumentError, "Cannot apply multiple authentications to database '#{db_name}'"
      end

      auth = {
        :db_name  => db_name,
        :username => username,
        :password => password
      }
      @auths << auth
      auth
    end

    # Remove a saved authentication for this connection.
    #
    # @param [String] db_name
    #
    # @return [Boolean]
    def remove_auth(db_name)
      return unless @auths
      @auths.reject! { |a| a[:db_name] == db_name } ? true : false
    end

    # Remove all authentication information stored in this connection.
    #
    # @return [true] this operation return true because it always succeeds.
    def clear_auths
      @auths = []
      true
    end

    def authenticate_pools
      @primary_pool.authenticate_existing
    end

    def logout_pools(db)
      @primary_pool.logout_existing(db)
    end

    # Return a hash with all database names
    # and their respective sizes on disk.
    #
    # @return [Hash]
    def database_info
      doc = self['admin'].command({:listDatabases => 1})
      doc['databases'].each_with_object({}) do |db, info|
        info[db['name']] = db['sizeOnDisk'].to_i
      end
    end

    # Return an array of database names.
    #
    # @return [Array]
    def database_names
      database_info.keys
    end

    # Return a database with the given name.
    # See DB#new for valid options hash parameters.
    #
    # @param [String] db_name a valid database name.
    # @param [Hash] opts options to be passed to the DB constructor.
    #
    # @return [Mongo::DB]
    #
    # @core databases db-instance_method
    def db(db_name=nil, opts={})
      if !db_name && uri = ENV['MONGODB_URI']
        db_name = uri[%r{/([^/\?]+)(\?|$)}, 1]
      end

      db_name ||= DEFAULT_DB_NAME

      DB.new(db_name, self, opts)
    end

    # Shortcut for returning a database. Use DB#db to accept options.
    #
    # @param [String] db_name a valid database name.
    #
    # @return [Mongo::DB]
    #
    # @core databases []-instance_method
    def [](db_name)
      DB.new(db_name, self)
    end

    def refresh
    end

    def pinned_pool
      @primary_pool
    end

    def pin_pool(pool, read_prefs)
    end

    def unpin_pool
    end

    # Drop a database.
    #
    # @param [String] name name of an existing database.
    def drop_database(name)
      self[name].command(:dropDatabase => 1)
    end

    # Copy the database +from+ to +to+ on localhost. The +from+ database is
    # assumed to be on localhost, but an alternate host can be specified.
    #
    # @param [String] from name of the database to copy from.
    # @param [String] to name of the database to copy to.
    # @param [String] from_host host of the 'from' database.
    # @param [String] username username for authentication against from_db (>=1.3.x).
    # @param [String] password password for authentication against from_db (>=1.3.x).
    def copy_database(from, to, from_host=DEFAULT_HOST, username=nil, password=nil)
      oh = BSON::OrderedHash.new
      oh[:copydb]   = 1
      oh[:fromhost] = from_host
      oh[:fromdb]   = from
      oh[:todb]     = to
      if username || password
        unless username && password
          raise MongoArgumentError, "Both username and password must be supplied for authentication."
        end
        nonce_cmd = BSON::OrderedHash.new
        nonce_cmd[:copydbgetnonce] = 1
        nonce_cmd[:fromhost] = from_host
        result = self["admin"].command(nonce_cmd)
        oh[:nonce] = result["nonce"]
        oh[:username] = username
        oh[:key] = Mongo::Support.auth_key(username, password, oh[:nonce])
      end
      self["admin"].command(oh)
    end

    # Checks if a server is alive. This command will return immediately
    # even if the server is in a lock.
    #
    # @return [Hash]
    def ping
      self["admin"].command({:ping => 1})
    end

    # Get the build information for the current connection.
    #
    # @return [Hash]
    def server_info
      self["admin"].command({:buildinfo => 1})
    end

    # Get the build version of the current server.
    #
    # @return [Mongo::ServerVersion]
    #   object allowing easy comparability of version.
    def server_version
      ServerVersion.new(server_info["version"])
    end

    # Is it okay to connect to a slave?
    #
    # @return [Boolean]
    def slave_ok?
      @slave_ok
    end

    def mongos?
      @mongos
    end

    # Create a new socket and attempt to connect to master.
    # If successful, sets host and port to master and returns the socket.
    #
    # If connecting to a replica set, this method will replace the
    # initially-provided seed list with any nodes known to the set.
    #
    # @raise [ConnectionFailure] if unable to connect to any host or port.
    def connect
      close

      config = check_is_master(host_port)

      if config
        if config['ismaster'] == 1 || config['ismaster'] == true
          @read_primary = true
        elsif @slave_ok
          @read_primary = false
        end

        if config.has_key?('msg') && config['msg'] == 'isdbgrid'
          @mongos = true
        end

        @max_bson_size = config['maxBsonObjectSize']
        @max_message_size = config['maxMessageSizeBytes']
        set_primary(host_port)
      end

      if !connected?
        raise ConnectionFailure, "Failed to connect to a master node at #{host_port.join(":")}"
      end
    end
    alias :reconnect :connect

    # It's possible that we defined connected as all nodes being connected???
    # NOTE: Do check if this needs to be more stringent.
    # Probably not since if any node raises a connection failure, all nodes will be closed.
    def connected?
      @primary_pool && !@primary_pool.closed?
    end

    # Determine if the connection is active. In a normal case the *server_info* operation
    # will be performed without issues, but if the connection was dropped by the server or
    # for some reason the sockets are unsynchronized, a ConnectionFailure will be raised and
    # the return will be false.
    #
    # @return [Boolean]
    def active?
      return false unless connected?

      ping
      true

      rescue ConnectionFailure
      false
    end

    # Determine whether we're reading from a primary node. If false,
    # this connection connects to a secondary node and @slave_ok is true.
    #
    # @return [Boolean]
    def read_primary?
      @read_primary
    end
    alias :primary? :read_primary?

    # The socket pool that this connection reads from.
    #
    # @return [Mongo::Pool]
    def read_pool
      @primary_pool
    end

    # Close the connection to the database.
    def close
      @primary_pool.close if @primary_pool
      @primary_pool = nil
      @primary = nil
    end

    # Returns the maximum BSON object size as returned by the core server.
    # Use the 4MB default when the server doesn't report this.
    #
    # @return [Integer]
    def max_bson_size
      @max_bson_size || DEFAULT_MAX_BSON_SIZE
    end

    def max_message_size
      @max_message_size || max_bson_size * MESSAGE_SIZE_FACTOR
    end

    # Checkout a socket for reading (i.e., a secondary node).
    # Note: this is overridden in MongoReplicaSetClient.
    def checkout_reader(read_preference)
      connect unless connected?
      @primary_pool.checkout
    end

    # Checkout a socket for writing (i.e., a primary node).
    # Note: this is overridden in MongoReplicaSetClient.
    def checkout_writer
      connect unless connected?
      @primary_pool.checkout
    end

    # Check a socket back into its pool.
    # Note: this is overridden in MongoReplicaSetClient.
    def checkin(socket)
      if @primary_pool && socket && socket.pool
        socket.checkin
      end
    end

    protected

    def valid_opts
      GENERIC_OPTS +
      CLIENT_ONLY_OPTS +
      POOL_OPTS +
      READ_PREFERENCE_OPTS +
      WRITE_CONCERN_OPTS +
      TIMEOUT_OPTS
    end

    def check_opts(opts)
      bad_opts = opts.keys.reject { |opt| valid_opts.include?(opt) }

      unless bad_opts.empty?
        bad_opts.each {|opt| warn "#{opt} is not a valid option for #{self.class}"}
      end
    end

    # Parse option hash
    def setup(opts)
      @slave_ok = opts.delete(:slave_ok)

      @ssl = opts.delete(:ssl)
      @unix = @host ? @host.end_with?('.sock') : false

      if @ssl
        @socket_class = Mongo::SSLSocket
      elsif @unix
        @socket_class = Mongo::UNIXSocket
      else
        @socket_class = Mongo::TCPSocket
      end

      # Authentication objects
      @auths = opts.delete(:auths) || []

      # Pool size and timeout.
      @pool_size = opts.delete(:pool_size) || 1
      if opts[:timeout]
        warn "The :timeout option has been deprecated " +
          "and will be removed in the 2.0 release. Use :pool_timeout instead."
      end
      @pool_timeout = opts.delete(:pool_timeout) || opts.delete(:timeout) || 5.0

      # Timeout on socket read operation.
      @op_timeout = opts.delete(:op_timeout) || nil

      # Timeout on socket connect.
      @connect_timeout = opts.delete(:connect_timeout) || 30

      @logger = opts.delete(:logger) || nil

      if @logger
        write_logging_startup_message
      end

      # Determine read preference
      if defined?(@slave_ok) && (@slave_ok) || defined?(@read_secondary) && @read_secondary
        @read = :secondary_preferred
      else
        @read = opts.delete(:read) || :primary
      end
      Mongo::ReadPreference::validate(@read)

      @tag_sets = opts.delete(:tag_sets) || []
      @acceptable_latency = opts.delete(:secondary_acceptable_latency_ms) || 15

      # Connection level write concern options.
      @write_concern = get_write_concern(opts)

      connect if opts.fetch(:connect, true)
    end

    private

    def check_is_master(node)
      begin
        host, port = *node
        socket = nil
        config = nil

        socket = @socket_class.new(host, port, @op_timeout, @connect_timeout)
        if(@connect_timeout)
          Timeout::timeout(@connect_timeout, OperationTimeout) do
            config = self['admin'].command({:ismaster => 1}, :socket => socket)
          end
        else
          config = self['admin'].command({:ismaster => 1}, :socket => socket)
        end
      rescue OperationFailure, SocketError, SystemCallError, IOError
        close
      ensure
        if socket
          socket.close unless socket.closed?
        end
      end

      config
    end

    # Set the specified node as primary.
    def set_primary(node)
      host, port = *node
      @primary = [host, port]
      @primary_pool = Pool.new(self, host, port, :size => @pool_size, :timeout => @pool_timeout)
    end
  end
end
