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

require 'set'
require 'socket'
require 'thread'

module Mongo

  # Instantiates and manages connections to MongoDB.
  class Connection
    TCPSocket = ::TCPSocket
    Mutex = ::Mutex
    ConditionVariable = ::ConditionVariable

    # Abort connections if a ConnectionError is raised.
    Thread.abort_on_exception = true

    DEFAULT_PORT = 27017
    STANDARD_HEADER_SIZE = 16
    RESPONSE_HEADER_SIZE = 20

    MONGODB_URI_MATCHER = /(([-_.\w\d]+):([-_\w\d]+)@)?([-.\w\d]+)(:([\w\d]+))?(\/([-\d\w]+))?/
    MONGODB_URI_SPEC = "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/database]"

    attr_reader :logger, :size, :nodes, :auths, :primary, :secondaries, :arbiters,
      :safe, :primary_pool, :read_pool, :secondary_pools

    # Counter for generating unique request ids.
    @@current_request_id = 0

    # Create a connection to single MongoDB instance.
    #
    # You may specify whether connection to slave is permitted.
    # In all cases, the default host is "localhost" and the default port is 27017.
    #
    # To specify more than one host pair to be used as seeds in a replica set,
    # use Connection.multi.
    #
    # Once connected to a replica set, you can find out which nodes are primary, secondary, and
    # arbiters with the corresponding accessors: Connection#primary, Connection#secondaries, and
    # Connection#arbiters. This is useful if your application needs to connect manually to nodes other
    # than the primary.
    #
    # @param [String, Hash] host.
    # @param [Integer] port specify a port number here if only one host is being specified.
    #
    # @option options [Boolean, Hash] :safe (false) Set the default safe-mode options
    #   propogated to DB objects instantiated off of this Connection. This
    #   default can be overridden upon instantiation of any DB by explicity setting a :safe value
    #   on initialization.
    # @option options [Boolean] :slave_ok (false) Must be set to +true+ when connecting
    #   to a single, slave node.
    # @option options [Logger, #debug] :logger (nil) Logger instance to receive driver operation log.
    # @option options [Integer] :pool_size (1) The maximum number of socket connections allowed per
    #   connection pool. Note: this setting is relevant only for multi-threaded applications.
    # @option options [Float] :timeout (5.0) When all of the connections a pool are checked out,
    #   this is the number of seconds to wait for a new connection to be released before throwing an exception.
    #   Note: this setting is relevant only for multi-threaded applications (which in Ruby are rare).
    #
    # @example localhost, 27017
    #   Connection.new
    #
    # @example localhost, 27017
    #   Connection.new("localhost")
    #
    # @example localhost, 3000, max 5 connections, with max 5 seconds of wait time.
    #   Connection.new("localhost", 3000, :pool_size => 5, :timeout => 5)
    #
    # @example localhost, 3000, where this node may be a slave
    #   Connection.new("localhost", 3000, :slave_ok => true)
    #
    # @see http://api.mongodb.org/ruby/current/file.REPLICA_SETS.html Replica sets in Ruby
    #
    # @raise [ReplicaSetConnectionError] This is raised if a replica set name is specified and the
    #   driver fails to connect to a replica set with that name.
    #
    # @core connections
    def initialize(host=nil, port=nil, options={})
      @auths        = []

      if block_given?
        @nodes = yield self
      else
        @nodes = format_pair(host, port)
      end

      # Host and port of current master.
      @host = @port = nil

      # Replica set name
      @replica_set_name = options[:rs_name]

      # Lock for request ids.
      @id_lock = Mutex.new

      # Pool size and timeout.
      @pool_size = options[:pool_size] || 1
      @timeout   = options[:timeout]   || 5.0

      # Mutex for synchronizing pool access
      @connection_mutex = Mutex.new

      # Global safe option. This is false by default.
      @safe = options[:safe] || false

      # Create a mutex when a new key, in this case a socket,
      # is added to the hash.
      @safe_mutexes = Hash.new { |h, k| h[k] = Mutex.new }

      # Condition variable for signal and wait
      @queue = ConditionVariable.new

      # slave_ok can be true only if one node is specified
      @slave_ok = options[:slave_ok]

      # Cache the various node types
      # when connecting to a replica set.
      @primary     = nil
      @secondaries = []
      @arbiters    = []

      # Connection pool for primay node
      @primary_pool    = nil

      # Connection pools for each secondary node
      @secondary_pools = []
      @read_pool = nil

      @logger   = options[:logger] || nil

      should_connect = options.fetch(:connect, true)
      connect if should_connect
    end

    # Initialize a connection to a MongoDB replica set using an array of seed nodes.
    #
    # The seed nodes specified will be used on the initial connection to the replica set, but note
    # that this list of nodes will be replced by the list of canonical nodes returned by running the
    # is_master command on the replica set.
    #
    # @param nodes [Array] An array of arrays, each of which specifies a host and port.
    # @param opts [Hash] Any of the available options that can be passed to Connection.new.
    #
    # @option options [String] :rs_name (nil) The name of the replica set to connect to. An exception will be
    #   raised if unable to connect to a replica set with this name.
    # @option options [Boolean] :read_secondary (false) When true, this connection object will pick a random slave
    #   to send reads to.
    #
    # @example
    #   Connection.multi([["db1.example.com", 27017], ["db2.example.com", 27017]])
    #
    # @example This connection will read from a random secondary node.
    #   Connection.multi([["db1.example.com", 27017], ["db2.example.com", 27017], ["db3.example.com", 27017]],
    #                   :read_secondary => true)
    #
    # @return [Mongo::Connection]
    def self.multi(nodes, opts={})
      unless nodes.length > 0 && nodes.all? {|n| n.is_a? Array}
        raise MongoArgumentError, "Connection.multi requires at least one node to be specified."
      end

      # Block returns an array, the first element being an array of nodes and the second an array
      # of authorizations for the database.
      new(nil, nil, opts) do |con|
        nodes.map do |node|
          con.instance_variable_set(:@replica_set, true)
          con.instance_variable_set(:@read_secondary, true) if opts[:read_secondary]
          con.pair_val_to_connection(node)
        end
      end
    end

    # Initialize a connection to MongoDB using the MongoDB URI spec:
    #
    # @param uri [String]
    #   A string of the format mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/database]
    #
    # @param opts Any of the options available for Connection.new
    #
    # @return [Mongo::Connection]
    def self.from_uri(uri, opts={})
      new(nil, nil, opts) do |con|
        con.parse_uri(uri)
      end
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
      self['admin']['$cmd.sys.inprog'].find_one['fsyncLock'] == 1
    end

    # Unlock a previously fsync-locked mongod process.
    #
    # @return [BSON::OrderedHash] command response
    def unlock!
      self['admin']['$cmd.sys.unlock'].find_one
    end

    # Apply each of the saved database authentications.
    #
    # @return [Boolean] returns true if authentications exist and succeeed, false
    #   if none exists.
    #
    # @raise [AuthenticationError] raises an exception if any one
    #   authentication fails.
    def apply_saved_authentication
      return false if @auths.empty?
      @auths.each do |auth|
        self[auth['db_name']].authenticate(auth['username'], auth['password'], false)
      end
      true
    end

    # Save an authentication to this connection. When connecting,
    # the connection will attempt to re-authenticate on every db
    # specificed in the list of auths. This method is called automatically
    # by DB#authenticate.
    #
    # Note: this method will not actually issue an authentication command. To do that,
    # either run Connection#apply_saved_authentication or DB#authenticate.
    #
    # @param [String] db_name
    # @param [String] username
    # @param [String] password
    #
    # @return [Hash] a hash representing the authentication just added.
    def add_auth(db_name, username, password)
      remove_auth(db_name)
      auth = {}
      auth['db_name']  = db_name
      auth['username'] = username
      auth['password'] = password
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
      if @auths.reject! { |a| a['db_name'] == db_name }
        true
      else
        false
      end
    end

    # Remove all authenication information stored in this connection.
    #
    # @return [true] this operation return true because it always succeeds.
    def clear_auths
      @auths = []
      true
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
    #
    # @return [Mongo::DB]
    #
    # @core databases db-instance_method
    def db(db_name, options={})
      DB.new(db_name, self, options)
    end

    # Shortcut for returning a database. Use DB#db to accept options.
    #
    # @param [String] db_name a valid database name.
    #
    # @return [Mongo::DB]
    #
    # @core databases []-instance_method
    def [](db_name)
      DB.new(db_name, self, :safe => @safe)
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
    def copy_database(from, to, from_host="localhost", username=nil, password=nil)
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

    # Increment and return the next available request id.
    #
    # return [Integer]
    def get_request_id
      request_id = ''
      @id_lock.synchronize do
        request_id = @@current_request_id += 1
      end
      request_id
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
      @read_secondary || @slave_ok
    end

    # Send a message to MongoDB, adding the necessary headers.
    #
    # @param [Integer] operation a MongoDB opcode.
    # @param [BSON::ByteBuffer] message a message to send to the database.
    #
    # @return [Integer] number of bytes sent
    def send_message(operation, message, log_message=nil)
      begin
        packed_message = add_message_headers(operation, message).to_s
        socket = checkout_writer
        send_message_on_socket(packed_message, socket)
      ensure
        checkin_writer(socket)
      end
    end

    # Sends a message to the database, waits for a response, and raises
    # an exception if the operation has failed.
    #
    # @param [Integer] operation a MongoDB opcode.
    # @param [BSON::ByteBuffer] message a message to send to the database.
    # @param [String] db_name the name of the database. used on call to get_last_error.
    # @param [Hash] last_error_params parameters to be sent to getLastError. See DB#error for
    #   available options.
    #
    # @see DB#get_last_error for valid last error params.
    #
    # @return [Hash] The document returned by the call to getlasterror.
    def send_message_with_safe_check(operation, message, db_name, log_message=nil, last_error_params=false)
      message_with_headers = add_message_headers(operation, message)
      message_with_check   = last_error_message(db_name, last_error_params)
      begin
        sock = checkout_writer
        packed_message = message_with_headers.append!(message_with_check).to_s
        docs = num_received = cursor_id = ''
        @safe_mutexes[sock].synchronize do
          send_message_on_socket(packed_message, sock)
          docs, num_received, cursor_id = receive(sock)
        end
      ensure
        checkin_writer(sock)
      end

      if num_received == 1 && (error = docs[0]['err'] || docs[0]['errmsg'])
        close if error == "not master"
        raise Mongo::OperationFailure, docs[0]['code'].to_s + ': ' + error
      end

      docs[0]
    end

    # Sends a message to the database and waits for the response.
    #
    # @param [Integer] operation a MongoDB opcode.
    # @param [BSON::ByteBuffer] message a message to send to the database.
    # @param [Socket] socket a socket to use in lieu of checking out a new one.
    #
    # @return [Array]
    #   An array whose indexes include [0] documents returned, [1] number of document received,
    #   and [3] a cursor_id.
    def receive_message(operation, message, log_message=nil, socket=nil, command=false)
      packed_message = add_message_headers(operation, message).to_s
      begin
        sock = socket || (command ? checkout_writer : checkout_reader)

        result = ''
        @safe_mutexes[sock].synchronize do
          send_message_on_socket(packed_message, sock)
          result = receive(sock)
        end
      ensure
        command ? checkin_writer(sock) : checkin_reader(sock)
      end
      result
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

      raise ConnectionFailure, "failed to connect to any given host:port" unless connected?
    end

    def connecting?
      @nodes_to_try.length > 0
    end

    # It's possible that we defined connected as all nodes being connected???
    # NOTE: Do check if this needs to be more stringent.
    # Probably not since if any node raises a connection failure, all nodes will be closed.
    def connected?
      @primary_pool && @primary_pool.host && @primary_pool.port
    end

    # Close the connection to the database.
    def close
      @primary_pool.close if @primary_pool
      @primary_pool = nil
      @read_pool    = nil
      @secondary_pools.each do |pool|
        pool.close
      end
    end

    ## Configuration helper methods

    # Returns an array of host-port pairs.
    #
    # @private
    def format_pair(pair_or_host, port)
      case pair_or_host
        when String
          [[pair_or_host, port ? port.to_i : DEFAULT_PORT]]
        when nil
          [['localhost', DEFAULT_PORT]]
      end
    end

    # Convert an argument containing a host name string and a
    # port number integer into a [host, port] pair array.
    #
    # @private
    def pair_val_to_connection(a)
      case a
      when nil
        ['localhost', DEFAULT_PORT]
      when String
        [a, DEFAULT_PORT]
      when Integer
        ['localhost', a]
      when Array
        a
      end
    end

    # Parse a MongoDB URI. This method is used by Connection.from_uri.
    # Returns an array of nodes and an array of db authorizations, if applicable.
    #
    # @private
    def parse_uri(string)
      if string =~ /^mongodb:\/\//
        string = string[10..-1]
      else
        raise MongoArgumentError, "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
      end

      nodes = []
      auths = []
      specs = string.split(',')
      specs.each do |spec|
        matches  = MONGODB_URI_MATCHER.match(spec)
        if !matches
          raise MongoArgumentError, "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
        end

        uname = matches[2]
        pwd   = matches[3]
        host  = matches[4]
        port  = matches[6] || DEFAULT_PORT
        if !(port.to_s =~ /^\d+$/)
          raise MongoArgumentError, "Invalid port #{port}; port must be specified as digits."
        end
        port  = port.to_i
        db    = matches[8]

        if uname && pwd && db
          add_auth(db, uname, pwd)
        elsif uname || pwd || db
          raise MongoArgumentError, "MongoDB URI must include all three of username, password, " +
            "and db if any one of these is specified."
        end

        nodes << [host, port]
      end

      nodes
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

    private

    # Pick a node randomly from the set of possibly secondaries.
    def pick_secondary_for_read
      if (size = @secondary_pools.size) > 1
        @read_pool = @secondary_pools[rand(size)]
      end
    end

    # If a ConnectionFailure is raised, this method will be called
    # to close the connection and reset connection values.
    def reset_connection
      close
      @primary = nil
      @secondaries     = []
      @secondary_pools = []
      @arbiters        = []
      @nodes_tried     = []
      @nodes_to_try    = []
    end

    # Primary is defined as either a master node or a slave if
    # :slave_ok has been set to +true+.
    #
    # If a primary node is discovered, we set the the @host and @port and
    # apply any saved authentication.
    def is_primary?(config)
      config && (config['ismaster'] == 1 || config['ismaster'] == true) || !@replica_set && @slave_ok
    end

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
          update_node_list(config['hosts']) if config['hosts']

          if config['msg'] && @logger
            @logger.warn("MONGODB #{config['msg']}")
          end
        end

        socket.close if socket
      end

      config
    end

    # Make sure that we're connected to the expected replica set.
    def check_set_name(config, socket)
      if @replica_set_name
        config = self['admin'].command({:replSetGetStatus => 1},
                   :sock => socket, :check_response => false)

        if !Mongo::Support.ok?(config)
          raise ReplicaSetConnectionError, config['errmsg']
        elsif config['set'] != @replica_set_name
          raise ReplicaSetConnectionError,
            "Attempting to connect to replica set '#{config['set']}' but expected '#{@replica_set_name}'"
        end
      end
    end

    # Set the specified node as primary, and
    # apply any saved authentication credentials.
    def set_primary(node)
      host, port = *node
      @primary = [host, port]
      @primary_pool = Pool.new(self, host, port, :size => @pool_size, :timeout => @timeout)
      apply_saved_authentication
    end

    # Determines what kind of node we have and caches its host
    # and port so that users can easily connect manually.
    def set_auxillary(node, config)
      if config
        if config['secondary']
          host, port = *node
          @secondaries << node unless @secondaries.include?(node)
          if @read_secondary
            @secondary_pools << Pool.new(self, host, port, :size => @pool_size, :timeout => @timeout)
          end
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
        [host, port.to_i]
      end

      # Replace the list of seed nodes with the canonical list.
      @nodes = new_nodes.clone

      @nodes_to_try = new_nodes - @nodes_tried
    end

    def receive(sock)
      receive_and_discard_header(sock)
      number_received, cursor_id = receive_response_header(sock)
      read_documents(number_received, cursor_id, sock)
    end

    def receive_header(sock)
      header = BSON::ByteBuffer.new
      header.put_binary(receive_message_on_socket(16, sock))
      unless header.size == STANDARD_HEADER_SIZE
        raise "Short read for DB response header: " +
          "expected #{STANDARD_HEADER_SIZE} bytes, saw #{header.size}"
      end
      header.rewind
      size        = header.get_int
      request_id  = header.get_int
      response_to = header.get_int
      op          = header.get_int
    end

    def receive_and_discard_header(sock)
      bytes_read = receive_and_discard_message_on_socket(16, sock)
      unless bytes_read == STANDARD_HEADER_SIZE
        raise "Short read for DB response header: " +
          "expected #{STANDARD_HEADER_SIZE} bytes, saw #{bytes_read}"
      end
      nil
    end

    def receive_response_header(sock)
      header_buf = receive_message_on_socket(RESPONSE_HEADER_SIZE, sock)
      if header_buf.length != RESPONSE_HEADER_SIZE
        raise "Short read for DB response header; " +
          "expected #{RESPONSE_HEADER_SIZE} bytes, saw #{header_buf.length}"
      end
      flags, cursor_id_a, cursor_id_b, starting_from, number_remaining = header_buf.unpack('VVVVV')
      check_response_flags(flags)
      cursor_id = (cursor_id_b << 32) + cursor_id_a
      [number_remaining, cursor_id]
    end

    def check_response_flags(flags)
      if flags & Mongo::Constants::REPLY_CURSOR_NOT_FOUND != 0
        raise Mongo::OperationFailure, "Query response returned CURSOR_NOT_FOUND. " +
          "Either an invalid cursor was specified, or the cursor may have timed out on the server."
      elsif flags & Mongo::Constants::REPLY_QUERY_FAILURE != 0
        # Getting odd failures when a exception is raised here.
      end
    end

    def read_documents(number_received, cursor_id, sock)
      docs = []
      number_remaining = number_received
      while number_remaining > 0 do
        buf = receive_message_on_socket(4, sock)
        size = buf.unpack('V')[0]
        buf << receive_message_on_socket(size - 4, sock)
        number_remaining -= 1
        docs << BSON::BSON_CODER.deserialize(buf)
      end
      [docs, number_received, cursor_id]
    end

    # Constructs a getlasterror message. This method is used exclusively by
    # Connection#send_message_with_safe_check.
    def last_error_message(db_name, opts)
      message = BSON::ByteBuffer.new
      message.put_int(0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{db_name}.$cmd")
      message.put_int(0)
      message.put_int(-1)
      cmd = BSON::OrderedHash.new
      cmd[:getlasterror] = 1
      if opts.is_a?(Hash)
        opts.assert_valid_keys(:w, :wtimeout, :fsync)
        cmd.merge!(opts)
      end
      message.put_binary(BSON::BSON_CODER.serialize(cmd, false).to_s)
      add_message_headers(Mongo::Constants::OP_QUERY, message)
    end

    # Prepares a message for transmission to MongoDB by
    # constructing a valid message header.
    def add_message_headers(operation, message)
      headers = [
        # Message size.
        16 + message.size,

        # Unique request id.
        get_request_id,

        # Response id.
        0,

        # Opcode.
        operation
      ].pack('VVVV')

      message.prepend!(headers)
    end

    # Low-level method for sending a message on a socket.
    # Requires a packed message and an available socket,
    #
    # @return [Integer] number of bytes sent
    def send_message_on_socket(packed_message, socket)
      begin
      total_bytes_sent = socket.send(packed_message, 0)
      if total_bytes_sent != packed_message.size
        packed_message.slice!(0, total_bytes_sent)
        while packed_message.size > 0
          byte_sent = socket.send(packed_message, 0)
          total_bytes_sent += byte_sent
          packed_message.slice!(0, byte_sent)
        end
      end
      total_bytes_sent
      rescue => ex
        close
        raise ConnectionFailure, "Operation failed with the following exception: #{ex}"
      end
    end

    # Low-level method for receiving data from socket.
    # Requires length and an available socket.
    def receive_message_on_socket(length, socket)
      begin
        message = socket.read(length)
        raise ConnectionFailure, "connection closed" unless message.length > 0
        if message.length < length
          chunk = new_binary_string
          while message.length < length
            socket.read(length - message.length, chunk)
            raise ConnectionFailure, "connection closed" unless chunk.length > 0
            message << chunk
          end
        end
        rescue => ex
          close
          raise ConnectionFailure, "Operation failed with the following exception: #{ex}"
      end
      message
    end

    # Low-level data for receiving data from socket.
    # Unlike #receive_message_on_socket, this method immediately discards the data
    # and only returns the number of bytes read.
    def receive_and_discard_message_on_socket(length, socket)
      bytes_read = 0
      begin
        chunk = socket.read(length)
        bytes_read = chunk.length
        raise ConnectionFailure, "connection closed" unless bytes_read > 0
        if bytes_read < length
          while bytes_read < length
            socket.read(length - bytes_read, chunk)
            raise ConnectionFailure, "connection closed" unless chunk.length > 0
            bytes_read += chunk.length
          end
        end
        rescue => ex
          close
          raise ConnectionFailure, "Operation failed with the following exception: #{ex}"
      end
      bytes_read
    end

    if defined?(Encoding)
      BINARY_ENCODING = Encoding.find("binary")

      def new_binary_string
        "".force_encoding(BINARY_ENCODING)
      end
    else
      def new_binary_string
        ""
      end
    end
  end
end
