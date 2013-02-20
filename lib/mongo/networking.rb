module Mongo
  module Networking

    STANDARD_HEADER_SIZE = 16
    RESPONSE_HEADER_SIZE = 20

    # Counter for generating unique request ids.
    @@current_request_id = 0

    # Send a message to MongoDB, adding the necessary headers.
    #
    # @param [Integer] operation a MongoDB opcode.
    # @param [BSON::ByteBuffer] message a message to send to the database.
    #
    # @option opts [Symbol] :connection (:writer) The connection to which
    #   this message should be sent. Valid options are :writer and :reader.
    #
    # @return [Integer] number of bytes sent
    def send_message(operation, message, opts={})
      if opts.is_a?(String)
        warn "MongoClient#send_message no longer takes a string log message. " +
          "Logging is now handled within the Collection and Cursor classes."
        opts = {}
      end

      add_message_headers(message, operation)
      packed_message = message.to_s

      sock = nil
      pool = opts.fetch(:pool, nil)
      begin
        if pool
          #puts "send_message pool.port:#{pool.port}"
          sock = pool.checkout
        else
          sock ||= checkout_writer
        end
        send_message_on_socket(packed_message, sock)
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        close
        raise ex
      ensure
        if sock
          sock.checkin
        end
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
    def send_message_with_gle(operation, message, db_name, log_message=nil, write_concern=false)
      docs = num_received = cursor_id = ''
      add_message_headers(message, operation)

      last_error_message = build_get_last_error_message(db_name, write_concern)
      last_error_id = add_message_headers(last_error_message, Mongo::Constants::OP_QUERY)

      packed_message = message.append!(last_error_message).to_s
      sock = nil
      begin
        sock = checkout_writer
        send_message_on_socket(packed_message, sock)
        docs, num_received, cursor_id = receive(sock, last_error_id)
        checkin(sock)
      rescue ConnectionFailure, OperationFailure, OperationTimeout => ex
        checkin(sock)
        raise ex
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        close
        raise ex
      end

      if num_received == 1 && (error = docs[0]['err'] || docs[0]['errmsg'])
        if error.include?("not master")
          close
          raise ConnectionFailure.new(docs[0]['code'].to_s + ': ' + error, docs[0]['code'], docs[0])
        else
          error = "wtimeout" if error == "timeout"
          raise OperationFailure.new(docs[0]['code'].to_s + ': ' + error, docs[0]['code'], docs[0])
        end
      end

      docs[0]
    end

    # Sends a message to the database and waits for the response.
    #
    # @param [Integer] operation a MongoDB opcode.
    # @param [BSON::ByteBuffer] message a message to send to the database.
    # @param [String] log_message this is currently a no-op and will be removed.
    # @param [Socket] socket a socket to use in lieu of checking out a new one.
    # @param [Boolean] command (false) indicate whether this is a command. If this is a command,
    #   the message will be sent to the primary node.
    # @param [Boolean] command (false) indicate whether the cursor should be exhausted. Set
    #   this to true only when the OP_QUERY_EXHAUST flag is set.
    #
    # @return [Array]
    #   An array whose indexes include [0] documents returned, [1] number of document received,
    #   and [3] a cursor_id.
    def receive_message(operation, message, log_message=nil, socket=nil, command=false,
                        read=:primary, exhaust=false)
      request_id = add_message_headers(message, operation)
      packed_message = message.to_s

      result = ''

      begin
        send_message_on_socket(packed_message, socket)
        result = receive(socket, request_id, exhaust)
      rescue ConnectionFailure => ex
        socket.close
        checkin(socket)
        raise ex
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        close
        raise ex
      rescue Exception => ex
        if defined?(IRB)
          close if ex.class == IRB::Abort
        end
        raise ex
      end
      result
    end

    private

    def receive(sock, cursor_id, exhaust=false)
      if exhaust
        docs = []
        num_received = 0

        while(cursor_id != 0) do
          receive_header(sock, cursor_id, exhaust)
          number_received, cursor_id = receive_response_header(sock)
          new_docs, n = read_documents(number_received, sock)
          docs += new_docs
          num_received += n
        end

        return [docs, num_received, cursor_id]
      else
        receive_header(sock, cursor_id, exhaust)
        number_received, cursor_id = receive_response_header(sock)
        docs, num_received = read_documents(number_received, sock)

        return [docs, num_received, cursor_id]
      end
    end

    def receive_header(sock, expected_response, exhaust=false)
      header = receive_message_on_socket(16, sock)

      # unpacks to size, request_id, response_to
      response_to = header.unpack('VVV')[2]
      if !exhaust && expected_response != response_to
        raise Mongo::ConnectionFailure, "Expected response #{expected_response} but got #{response_to}"
      end

      unless header.size == STANDARD_HEADER_SIZE
        raise "Short read for DB response header: " +
          "expected #{STANDARD_HEADER_SIZE} bytes, saw #{header.size}"
      end
      nil
    end

    def receive_response_header(sock)
      header_buf = receive_message_on_socket(RESPONSE_HEADER_SIZE, sock)
      if header_buf.length != RESPONSE_HEADER_SIZE
        raise "Short read for DB response header; " +
          "expected #{RESPONSE_HEADER_SIZE} bytes, saw #{header_buf.length}"
      end

      # unpacks to flags, cursor_id_a, cursor_id_b, starting_from, number_remaining
      flags, cursor_id_a, cursor_id_b, _, number_remaining = header_buf.unpack('VVVVV')

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

    def read_documents(number_received, sock)
      docs = []
      number_remaining = number_received
      while number_remaining > 0 do
        buf = receive_message_on_socket(4, sock)
        size = buf.unpack('V')[0]
        buf << receive_message_on_socket(size - 4, sock)
        number_remaining -= 1
        docs << BSON::BSON_CODER.deserialize(buf)
      end
      [docs, number_received]
    end

    def build_command_message(db_name, query, projection=nil, skip=0, limit=-1)
      message = BSON::ByteBuffer.new("", max_message_size)
      message.put_int(0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{db_name}.$cmd")
      message.put_int(skip)
      message.put_int(limit)
      message.put_binary(BSON::BSON_CODER.serialize(query, false, false, max_bson_size).to_s)
      message.put_binary(BSON::BSON_CODER.serialize(projection, false, false, max_bson_size).to_s) if projection
      message
    end

    # Constructs a getlasterror message. This method is used exclusively by
    # MongoClient#send_message_with_gle.
    def build_get_last_error_message(db_name, write_concern)
      gle = BSON::OrderedHash.new
      gle[:getlasterror] = 1
      if write_concern.is_a?(Hash)
        write_concern.assert_valid_keys(:w, :wtimeout, :fsync, :j)
        gle.merge!(write_concern)
        gle.delete(:w) if gle[:w] == 1
      end
      gle[:w] = gle[:w].to_s if gle[:w].is_a?(Symbol)
      build_command_message(db_name, gle)
    end

    # Prepares a message for transmission to MongoDB by
    # constructing a valid message header.
    #
    # Note: this method modifies message by reference.
    #
    # @return [Integer] the request id used in the header
    def add_message_headers(message, operation)
      headers = [
        # Message size.
        16 + message.size,

        # Unique request id.
        request_id = get_request_id,

        # Response id.
        0,

        # Opcode.
        operation
      ].pack('VVVV')

      message.prepend!(headers)

      request_id
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

    # Low-level method for sending a message on a socket.
    # Requires a packed message and an available socket,
    #
    # @return [Integer] number of bytes sent
    def send_message_on_socket(packed_message, socket)
      begin
      total_bytes_sent = socket.send(packed_message)
      if total_bytes_sent != packed_message.size
        packed_message.slice!(0, total_bytes_sent)
        while packed_message.size > 0
          byte_sent = socket.send(packed_message)
          total_bytes_sent += byte_sent
          packed_message.slice!(0, byte_sent)
        end
      end
      total_bytes_sent
      rescue => ex
        socket.close
        raise ConnectionFailure, "Operation failed with the following exception: #{ex}:#{ex.message}"
      end
    end

    # Low-level method for receiving data from socket.
    # Requires length and an available socket.
    def receive_message_on_socket(length, socket)
      begin
          message = receive_data(length, socket)
      rescue OperationTimeout, ConnectionFailure => ex
        socket.close

        if ex.class == OperationTimeout
          raise OperationTimeout, "Timed out waiting on socket read."
        else
          raise ConnectionFailure, "Operation failed with the following exception: #{ex}"
        end
      end
      message
    end

    def receive_data(length, socket)
      message = new_binary_string
      socket.read(length, message)

      raise ConnectionFailure, "connection closed" unless message && message.length > 0
      if message.length < length
        chunk = new_binary_string
        while message.length < length
          socket.read(length - message.length, chunk)
          raise ConnectionFailure, "connection closed" unless chunk.length > 0
          message << chunk
        end
      end
      message
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
