# --
# Copyright (C) 2008-2009 10gen Inc.
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
require 'monitor'

module Mongo

  # A connection to MongoDB.
  class Connection

    # We need to make sure that all connection abort when
    # a ConnectionError is raised.
    Thread.abort_on_exception = true

    DEFAULT_PORT = 27017
    STANDARD_HEADER_SIZE = 16
    RESPONSE_HEADER_SIZE = 20

    attr_reader :logger, :size, :host, :port, :nodes, :sockets, :checked_out

    def slave_ok?
      @slave_ok
    end

    # Counter for generating unique request ids.
    @@current_request_id = 0

    # Creates a connection to MongoDB. Specify either one or a pair of servers,
    # along with a maximum connection pool size and timeout.
    #
    # == Connecting
    # If connecting to just one server, you may specify whether connection to slave is permitted.
    #
    # In all cases, the default host is "localhost" and the default port, is 27017.
    #
    # When specifying a pair, pair_or_host, is a hash with two keys: :left and :right. Each key maps to either
    # * a server name, in which case port is 27017,
    # * a port number, in which case the server is "localhost", or
    # * an array containing [server_name, port_number]
    #
    # === Options
    #
    # :slave_ok :: Defaults to +false+. Must be set to +true+ when connecting
    #              to a single, slave node.
    #
    # :logger :: Optional Logger instance to which driver usage information
    #            will be logged.
    #
    # :auto_reconnect :: DEPRECATED. See http://www.mongodb.org/display/DOCS/Replica+Pairs+in+Ruby
    #
    # :pool_size :: The maximum number of socket connections that can be opened
    #               that can be opened to the database.
    #
    # :timeout   :: When all of the connections to the pool are checked out,
    #               this is the number of seconds to wait for a new connection
    #               to be released before throwing an exception.
    #
    # === Examples:
    #
    #  # localhost, 27017
    #  Connection.new
    #
    #  # localhost, 27017
    #  Connection.new("localhost")
    #
    #  # localhost, 3000, max 5 connections, with max 5 seconds of wait time.
    #  Connection.new("localhost", 3000, :pool_size => 5, :timeout => 5)
    #
    #  # localhost, 3000, where this node may be a slave
    #  Connection.new("localhost", 3000, :slave_ok => true)
    #
    #  # A pair of servers. The driver will always talk to master.
    #  # On connection errors, Mongo::ConnectionFailure will be raised.
    #  # See http://www.mongodb.org/display/DOCS/Replica+Pairs+in+Ruby
    #  Connection.new({:left  => ["db1.example.com", 27017],
    #                  :right => ["db2.example.com", 27017]})
    #
    #  A pair of servers, with connection pooling. Not the nil param placeholder for port.
    #  Connection.new({:left  => ["db1.example.com", 27017],
    #                  :right => ["db2.example.com", 27017]}, nil,
    #                  :pool_size => 20, :timeout => 5)
    def initialize(pair_or_host=nil, port=nil, options={})
      @nodes = format_pair(pair_or_host, port)

      # Host and port of current master.
      @host = @port = nil

      # Lock for request ids.
      @id_lock = Mutex.new

      # Pool size and timeout.
      @size      = options[:pool_size] || 1
      @timeout   = options[:timeout]   || 5.0

      # Number of seconds to wait for threads to signal availability.
      @thread_timeout = @timeout >= 5.0 ? (@timeout / 4.0) : 1.0

      # Mutex for synchronizing pool access
      @connection_mutex = Monitor.new

      # Condition variable for signal and wait
      @queue = @connection_mutex.new_cond

      @sockets      = []
      @checked_out  = []

      if options[:auto_reconnect]
        warn(":auto_reconnect is deprecated. see http://www.mongodb.org/display/DOCS/Replica+Pairs+in+Ruby")
      end

      # Slave ok can be true only if one node is specified
      @slave_ok = options[:slave_ok] && @nodes.length == 1
      @logger   = options[:logger] || nil
      @options  = options

      should_connect = options[:connect].nil? ? true : options[:connect]
      connect_to_master if should_connect
    end

    # Returns a hash with all database names and their respective sizes on
    # disk.
    def database_info
      doc = self['admin'].command(:listDatabases => 1)
      returning({}) do |info|
        doc['databases'].each { |db| info[db['name']] = db['sizeOnDisk'].to_i }
      end
    end

    # Returns an array of database names.
    def database_names
      database_info.keys
    end

    # Returns the database named +db_name+. The slave_ok and
    # See DB#new for other options you can pass in.
    def db(db_name, options={})
      DB.new(db_name, self, options.merge(:logger => @logger))
    end

    # Returns the database named +db_name+.
    def [](db_name)
      DB.new(db_name, self, :logger => @logger)
    end

    # Drops the database +name+.
    def drop_database(name)
      self[name].command(:dropDatabase => 1)
    end

    # Copies the database +from+ on the local server to +to+ on the specified +host+.
    # +host+ defaults to 'localhost' if no value is provided.
    def copy_database(from, to, host="localhost")
      oh = OrderedHash.new
      oh[:copydb]   = 1
      oh[:fromhost] = host
      oh[:fromdb]   = from
      oh[:todb]     = to
      self["admin"].command(oh)
    end

    # Increments and returns the next available request id.
    def get_request_id
      request_id = ''
      @id_lock.synchronize do
        request_id = @@current_request_id += 1
      end
      request_id
    end

    # Returns the build information for the current connection.
    def server_info
      db("admin").command({:buildinfo => 1}, {:admin => true, :check_response => true})
    end

    # Gets the build version of the current server.
    # Returns a ServerVersion object for comparability.
    def server_version
      ServerVersion.new(server_info["version"])
    end


    ## Connections and pooling ##

    # Sends a message to MongoDB.
    #
    # Takes a MongoDB opcode, +operation+, a message of class ByteBuffer,
    # +message+, and an optional formatted +log_message+.
    # Sends the message to the databse, adding the necessary headers.
    def send_message(operation, message, log_message=nil)
      @logger.debug("  MONGODB #{log_message || message}") if @logger
      packed_message = add_message_headers(operation, message).to_s
      socket = checkout
      send_message_on_socket(packed_message, socket)
      checkin(socket)
    end

    # Sends a message to the database, waits for a response, and raises
    # and exception if the operation has failed.
    #
    # Takes a MongoDB opcode, +operation+, a message of class ByteBuffer,
    # +message+, the +db_name+, and an optional formatted +log_message+.
    # Sends the message to the databse, adding the necessary headers.
    def send_message_with_safe_check(operation, message, db_name, log_message=nil)
      message_with_headers = add_message_headers(operation, message)
      message_with_check   = last_error_message(db_name)
      @logger.debug("  MONGODB #{log_message || message}") if @logger
      sock = checkout
      packed_message = message_with_headers.append!(message_with_check).to_s
      send_message_on_socket(packed_message, sock)
      docs, num_received, cursor_id = receive(sock)
      checkin(sock)
      if num_received == 1 && error = docs[0]['err']
        raise Mongo::OperationFailure, error
      end
      [docs, num_received, cursor_id]
    end

    # Sends a message to the database and waits for the response.
    #
    # Takes a MongoDB opcode, +operation+, a message of class ByteBuffer,
    # +message+, and an optional formatted +log_message+. This method
    # also takes an options socket for internal use with #connect_to_master.
    def receive_message(operation, message, log_message=nil, socket=nil)
      packed_message = add_message_headers(operation, message).to_s
      @logger.debug("  MONGODB #{log_message || message}") if @logger
      sock = socket || checkout

      send_message_on_socket(packed_message, sock)
      result = receive(sock)
      checkin(sock)
      result
    end

    # Creates a new socket and tries to connect to master.
    # If successful, sets @host and @port to master and returns the socket.
    def connect_to_master
      close
      @host = @port = nil
      for node_pair in @nodes
        host, port = *node_pair
        begin
          socket = TCPSocket.new(host, port)
          socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

          # If we're connected to master, set the @host and @port
          result = self['admin'].command({:ismaster => 1}, false, false, socket)
          if result['ok'] == 1 && ((is_master = result['ismaster'] == 1) || @slave_ok)
            @host, @port = host, port
          end

          # Note: slave_ok can be true only when connecting to a single node.
          if @nodes.length == 1 && !is_master && !@slave_ok
            raise ConfigurationError, "Trying to connect directly to slave; " +
              "if this is what you want, specify :slave_ok => true."
          end

          break if is_master || @slave_ok
        rescue SocketError, SystemCallError, IOError => ex
          socket.close if socket
          false
        end
      end
      raise ConnectionFailure, "failed to connect to any given host:port" unless socket
    end

    # Are we connected to MongoDB? This is determined by checking whether
    # @host and @port have values, since they're set to nil on calls to #close.
    def connected?
      @host && @port
    end

    # Close the connection to the database.
    def close
      @sockets.each do |sock|
        sock.close
      end
      @host = @port = nil
      @sockets.clear
      @checked_out.clear
    end

    private

    # Return a socket to the pool.
    def checkin(socket)
      @connection_mutex.synchronize do
        @checked_out.delete(socket)
        @queue.signal
      end
      true
    end

    # Adds a new socket to the pool and checks it out.
    #
    # This method is called exclusively from #obtain_socket;
    # therefore, it runs within a mutex, as it must.
    def checkout_new_socket
      begin
      socket = TCPSocket.new(@host, @port)
      socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      rescue => ex
        raise ConnectionFailure, "Failed to connect socket: #{ex}"
      end
      @sockets << socket
      @checked_out << socket
      socket
    end

    # Checks out the first available socket from the pool.
    #
    # This method is called exclusively from #obtain_socket;
    # therefore, it runs within a mutex, as it must.
    def checkout_existing_socket
      socket = (@sockets - @checked_out).first
      @checked_out << socket
      socket
    end

    # Check out an existing socket or create a new socket if the maximum
    # pool size has not been exceeded. Otherwise, wait for the next
    # available socket.
    def checkout
      connect_to_master if !connected?
      start_time = Time.now
      loop do
        if (Time.now - start_time) > @timeout
            raise ConnectionTimeoutError, "could not obtain connection within " +
              "#{@timeout} seconds. The max pool size is currently #{@size}; " +
              "consider increasing the pool size or timeout."
        end

        @connection_mutex.synchronize do
          socket = if @checked_out.size < @sockets.size
                     checkout_existing_socket
                   elsif @sockets.size < @size
                     checkout_new_socket
                   end

          return socket if socket
          wait
        end
      end
    end

    if RUBY_VERSION >= '1.9'
      # Ruby 1.9's Condition Variables don't support timeouts yet;
      # until they do, we'll make do with this hack.
      def wait
        Timeout.timeout(@thread_timeout) do
          @queue.wait
        end
      end
    else
      def wait
        @queue.wait(@thread_timeout)
      end
    end

    def receive(sock)
      receive_header(sock)
      number_received, cursor_id = receive_response_header(sock)
      read_documents(number_received, cursor_id, sock)
    end

    def receive_header(sock)
      header = ByteBuffer.new
      header.put_array(receive_message_on_socket(16, sock).unpack("C*"))
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

    def receive_response_header(sock)
      header_buf = ByteBuffer.new
      header_buf.put_array(receive_message_on_socket(RESPONSE_HEADER_SIZE, sock).unpack("C*"))
      if header_buf.length != RESPONSE_HEADER_SIZE
        raise "Short read for DB response header; " +
          "expected #{RESPONSE_HEADER_SIZE} bytes, saw #{header_buf.length}"
      end
      header_buf.rewind
      result_flags     = header_buf.get_int
      cursor_id        = header_buf.get_long
      starting_from    = header_buf.get_int
      number_remaining = header_buf.get_int
      [number_remaining, cursor_id]
    end

    def read_documents(number_received, cursor_id, sock)
      docs = []
      number_remaining = number_received
      while number_remaining > 0 do
        buf = ByteBuffer.new
        buf.put_array(receive_message_on_socket(4, sock).unpack("C*"))
        buf.rewind
        size = buf.get_int
        buf.put_array(receive_message_on_socket(size - 4, sock).unpack("C*"), 4)
        number_remaining -= 1
        buf.rewind
        docs << BSON.deserialize(buf)
      end
      [docs, number_received, cursor_id]
    end

    def last_error_message(db_name)
      message = ByteBuffer.new
      message.put_int(0)
      BSON_RUBY.serialize_cstr(message, "#{db_name}.$cmd")
      message.put_int(0)
      message.put_int(-1)
      message.put_array(BSON.serialize({:getlasterror => 1}, false).unpack("C*"))
      add_message_headers(Mongo::Constants::OP_QUERY, message)
    end

    # Prepares a message for transmission to MongoDB by
    # constructing a valid message header.
    def add_message_headers(operation, message)
      headers = ByteBuffer.new

      # Message size.
      headers.put_int(16 + message.size)

      # Unique request id.
      headers.put_int(get_request_id)

      # Response id.
      headers.put_int(0)

      # Opcode.
      headers.put_int(operation)
      message.prepend!(headers)
    end

    # Low-level method for sending a message on a socket.
    # Requires a packed message and an available socket,
    def send_message_on_socket(packed_message, socket)
      begin
      socket.send(packed_message, 0)
      rescue => ex
        close
        raise ConnectionFailure, "Operation failed with the following exception: #{ex}"
      end
    end

    # Low-level method for receiving data from socket.
    # Requires length and an available socket.
    def receive_message_on_socket(length, socket)
      message = ""
      begin
        while message.length < length do
          chunk = socket.recv(length - message.length)
          raise ConnectionFailure, "connection closed" unless chunk.length > 0
          message += chunk
        end
        rescue => ex
          raise ConnectionFailure, "Operation failed with the following exception: #{ex}"
      end
      message
    end


    ## Private helper methods

    # Returns an array of host-port pairs.
    def format_pair(pair_or_host, port)
      case pair_or_host
        when String
          [[pair_or_host, port ? port.to_i : DEFAULT_PORT]]
        when Hash
         connections = []
         connections << pair_val_to_connection(pair_or_host[:left])
         connections << pair_val_to_connection(pair_or_host[:right])
         connections
        when nil
          [['localhost', DEFAULT_PORT]]
      end
    end

    # Turns an array containing a host name string and a
    # port number integer into a [host, port] pair array.
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

  end
end
