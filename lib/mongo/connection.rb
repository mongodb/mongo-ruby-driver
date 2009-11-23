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

    DEFAULT_PORT = 27017
    STANDARD_HEADER_SIZE = 16
    RESPONSE_HEADER_SIZE = 20

    attr_reader :logger, :size, :host, :port, :nodes, :sockets, :checked_out, :reserved_connections

    def slave_ok? 
      @slave_ok
    end
    
    # Counter for generating unique request ids.
    @@current_request_id = 0

    # Create a Mongo database server instance. Specify either one or a
    # pair of servers. 
    #
    # If connecting to just one server, you may specify whether connection to slave is permitted.
    # 
    # In all cases, the default host is "localhost" and the default port, is 27017.
    #
    # When specifying, pair_or_host, is a hash with two keys: :left and :right. Each key maps to either
    # * a server name, in which case port is 27017,
    # * a port number, in which case the server is "localhost", or
    # * an array containing [server_name, port_number]
    #
    # +options+ 
    #
    # :slave_ok :: Defaults to +false+. Must be set to +true+ when connecting
    #              to a single, slave node.
    #
    # :logger :: Optional Logger instance to which driver usage information
    #            will be logged.
    #
    # :auto_reconnect :: DEPRECATED. See http://www.mongodb.org/display/DOCS/Replica+Pairs+in+Ruby
    #
    # Here are a few examples:
    #
    #   # localhost, 27017
    #   Connection.new
    #   
    #   # localhost, 27017
    #   Connection.new("localhost")
    #
    #   # localhost, 3000
    #   Connection.new("localhost", 3000)
    #
    #   # localhost, 3000, where this node may be a slave
    #   Connection.new("localhost", 3000, :slave_ok => true)
    #
    #  # A pair of servers. The driver will always talk to master. 
    #  # On connection errors, Mongo::ConnectionFailure will be raised.
    #  # See http://www.mongodb.org/display/DOCS/Replica+Pairs+in+Ruby 
    #  Connection.new({:left  => ["db1.example.com", 27017],
    #                  :right => ["db2.example.com", 27017]})
    def initialize(pair_or_host=nil, port=nil, options={})
      @nodes = format_pair(pair_or_host)

      # Host and port of current master.
      @host = @port = nil
      
      # Lock for request ids.
      @id_lock = Mutex.new

      # Lock for checking master.
      @master_lock = Mutex.new

      # Pool size and timeout.
      @size      = options[:pool_size] || 1
      @timeout   = options[:timeout]   || 1.0

      # Cache of reserved sockets mapped to threads
      @reserved_connections = {}

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

    # Return the database named +db_name+. The slave_ok and
    # See DB#new for other options you can pass in.
    def db(db_name, options={})
      DB.new(db_name, self, options.merge(:logger => @logger))
    end

    # Return the database named +db_name+.
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

    # Return the build information for the current connection.
    def server_info
      db("admin").command({:buildinfo => 1}, {:admin => true, :check_response => true})
    end

    # Get the build version of the current server.
    # Returns a ServerVersion object for comparability.
    def server_version
      ServerVersion.new(server_info["version"])
    end


    ## Connections and pooling ##
    
    # Sends a message to MongoDB.
    #
    # Takes a MongoDB opcode, +operation+, a message of class ByteBuffer,
    # +message+, and an optional formatted +log_message+.
    def send_message(operation, message, log_message=nil)
      @logger.debug("  MONGODB #{log_message || message}") if @logger

      packed_message = pack_message(operation, message)
      socket = checkout
      send_message_on_socket(packed_message, socket)
    end

    # Sends a message to MongoDB and returns the response.
    #
    # Takes a MongoDB opcode, +operation+, a message of class ByteBuffer,
    # +message+, an optional formatted +log_message+, and an optional
    # socket.
    def receive_message(operation, message, log_msg=nil, sock=nil)
      @logger.debug("  MONGODB #{log_message || message}") if @logger
      packed_message = pack_message(operation, message)

      # This code is used only if we're checking for master.
      if sock
        @master_lock.synchronize do 
          response = send_and_receive(packed_message, sock)
        end
      else
        socket = checkout
        response = send_and_receive(packed_message, socket)
      end
      response
    end

    # Sends a message to MongoDB.
    #
    # Takes a MongoDB opcode, +operation+, a message of class ByteBuffer,
    # +message+, and an optional formatted +log_message+.
    # Sends the message to the databse, adding the necessary headers.
    def send_message_with_operation(operation, message, log_message=nil)
      @logger.debug("  MONGODB #{log_message || message}") if @logger
      packed_message = pack_message(operation, message)
      socket = checkout
      response = send_message_on_socket(packed_message, socket)
      checkin(socket)
      response
    end

    # Sends a message to the database, waits for a response, and raises
    # and exception if the operation has failed.
    def send_message_with_safe_check(operation, message, db_name, log_message=nil)
      message_with_headers = add_message_headers(operation, message)
      message_with_check   = last_error_message(db_name)
      @logger.debug("  MONGODB #{log_message || message}") if @logger
      sock = checkout
      msg  = message_with_headers.append!(message_with_check).to_s
      send_message_on_socket(msg, sock)
      docs, num_received, cursor_id = receive(sock)
      if num_received == 1 && error = docs[0]['err']
        raise Mongo::OperationFailure, error
      end
      checkin(sock)
      [docs, num_received, cursor_id]
    end

    # Send a message to the database and waits for the response.
    def receive_message_with_operation(operation, message, log_message=nil, socket=nil)
      message_with_headers = add_message_headers(operation, message).to_s
      @logger.debug("  MONGODB #{log_message || message}") if @logger
      sock = socket || checkout

      send_message_on_socket(message_with_headers, sock)
      receive(sock)
    end

    # Creates a new socket and tries to connect to master.
    # If successful, sets @host and @port to master and returns the socket.
    def connect_to_master
      @host = @port = nil
      for node_pair in @nodes
        host, port = *node_pair
        begin
          socket = TCPSocket.new(host, port)
          socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

          result = self['admin'].command({:ismaster => 1}, false, false, socket)
          if result['ok'] == 1 && ((is_master = result['ismaster'] == 1) || @slave_ok)
            @host, @port = host, port
          end

          # Note: slave_ok can be true only when connecting to a single node.
          if @nodes.length > 1 && !is_master && !@slave_ok
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

    # NOTE: might not need this.
    # Are we connected to the master node?
    def master?
      doc = self['admin'].command(:ismaster => 1)
      doc['ok'] == 1 && doc['ismaster'] == 1
    end

    # NOTE: might not need this.
    # Returns a string of the form "host:port" that points to the master
    # database. Works even if this _is_ the master database.
    def master
      doc = self['admin'].command(:ismaster => 1)
      if doc['ok'] == 1 && doc['ismaster'] == 1
        "#@host:#@port"
      elsif doc['remote']
       doc['remote']
      else
        raise "Error retrieving master database: #{doc.inspect}"
      end
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
      @reserved_connections.clear
    end

    # Get a socket from the pool, mapped to the current thread.
    def checkout
      if sock = @reserved_connections[Thread.current.object_id]
        sock
      else
        sock = obtain_socket
        @reserved_connections[Thread.current.object_id] = sock
      end
      sock
    end

    # Return a socket to the pool.
    def checkin(socket)
      @connection_mutex.synchronize do 
        @checked_out.delete(socket)
        @reserved_connections.delete Thread.current.object_id
        @queue.signal
      end
    end

    # Releases the connection for any dead threads.
    # Called when the connection pool grows too large to free up more sockets.
    def clear_stale_cached_connections!
      keys = Set.new(@reserved_connections.keys)

      Thread.list.each do |thread|
        keys.delete(thread.object_id) if thread.alive?
      end
      
      keys.each do |key|
        next unless @reserved_connections.has_key?(key)
        checkin(@reserved_connections[key])
        @reserved_connections.delete(key)
      end
    end

    # Adds a new socket to the pool and checks it out.
    #
    # This method is called exclusively from #obtain_socket;
    # therefore, it runs within a mutex, as it must.
    def checkout_new_socket
      socket = TCPSocket.new(@host, @port)
      socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
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
    def obtain_socket
      @connection_mutex.synchronize do 
        
        # NOTE: Not certain that this is the best place for reconnect
        connect_to_master if !connected?
        loop do 
          socket = if @checked_out.size < @sockets.size
                     checkout_existing_socket
                   elsif @sockets.size < @size
                     checkout_new_socket
                   end

          return socket if socket
          # No connections available; wait.
          if @queue.wait(@timeout)
            next
          else
            # Try to clear out any stale threads to free up some connections
            clear_stale_cached_connections!
            if @size == @sockets.size
              raise ConnectionTimeoutError, "could not obtain connection within " +
                "#{@timeout} seconds. The max pool size is currently #{@size}; " +
                "consider increasing it."
            end
          end # if
        end # loop
      end #sync
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
        docs << BSON.new.deserialize(buf)
      end
      [docs, number_received, cursor_id]
    end

    def last_error_message(db_name)
      message = ByteBuffer.new
      message.put_int(0)
      BSON.serialize_cstr(message, "#{db_name}.$cmd")
      message.put_int(0)
      message.put_int(-1)
      message.put_array(BSON_SERIALIZER.serialize({:getlasterror => 1}, false).unpack("C*"))
      add_message_headers(Mongo::Constants::OP_QUERY, message)
    end

    # Prepares a message for transmission to MongoDB by
    # constructing a message header with a new request id.
    def pack_message(operation, message)
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
      message.to_s
    end

    # Low-level method for sending a message on a socket.
    # Requires a packed message and an available socket, 
    def send_message_on_socket(packed_message, socket)
      #socket will be connected to master when we receive it
      #begin
      socket.send(packed_message, 0)
      #rescue => ex
        # close
        # need to find a way to release the socket here
        # checkin(socket)
      #  raise ex
      #end
    end

    # Low-level method for receiving data from socket.
    # Requires length and an available socket.
    def receive_message_on_socket(length, socket)
      message = ""
      while message.length < length do
        chunk = socket.recv(length - message.length)
        raise "connection closed" unless chunk.length > 0
        message += chunk
      end
      message
    end


    ## Private helper methods

    # Returns an array of host-port pairs.
    def format_pair(pair_or_host)
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
