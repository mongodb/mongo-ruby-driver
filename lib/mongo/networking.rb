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
        warn "Connection#send_message no longer takes a string log message. " +
          "Logging is now handled within the Collection and Cursor classes."
        opts = {}
      end

      connection = opts.fetch(:connection, :writer)

      add_message_headers(message, operation)
      packed_message = message.to_s

      sock = nil
      begin
        if connection == :writer
          sock = checkout_writer
        else
          sock = checkout_reader
        end

        send_message_on_socket(packed_message, sock)
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        close
        raise ex
      ensure
        if sock
          if connection == :writer
            checkin_writer(sock)
          else
            checkin_reader(sock)
          end
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
    def send_message_with_safe_check(operation, message, db_name, log_message=nil, last_error_params=false)
      docs = num_received = cursor_id = ''
      add_message_headers(message, operation)

      last_error_message = BSON::ByteBuffer.new
      build_last_error_message(last_error_message, db_name, last_error_params)
      last_error_id = add_message_headers(last_error_message, Mongo::Constants::OP_QUERY)

      packed_message = message.append!(last_error_message).to_s
      sock = nil
      begin
        sock = checkout_writer
        send_message_on_socket(packed_message, sock)
        docs, num_received, cursor_id = receive(sock, last_error_id)
        checkin_writer(sock)
      rescue ConnectionFailure, OperationFailure, OperationTimeout => ex
        checkin_writer(sock)
        raise ex
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        close
        raise ex
      end

      if num_received == 1 && (error = docs[0]['err'] || docs[0]['errmsg'])
        close if error == "not master"
        error = "wtimeout" if error == "timeout"
        raise OperationFailure.new(docs[0]['code'].to_s + ': ' + error, docs[0]['code'], docs[0])
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
      sock   = nil
      begin
        if socket
          sock = socket
          should_checkin = false
        else
          if command || read == :primary
            sock = checkout_writer
          elsif read == :secondary
            sock = checkout_reader
          else
            sock = checkout_tagged(read)
          end
          should_checkin = true
        end

        send_message_on_socket(packed_message, sock)
        result = receive(sock, request_id, exhaust)
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        close
        raise ex
      ensure
        if should_checkin
          if command || read == :primary
            checkin_writer(sock)
          elsif read == :secondary
            checkin_reader(sock)
          else
            # TODO: sock = checkout_tagged(read)
          end
        end
      end
      result
    end

    private

    def receive(sock, cursor_id, exhaust=false)
      begin
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
      rescue Mongo::ConnectionFailure => ex
        close
        raise ex
      end
    end

    def receive_header(sock, expected_response, exhaust=false)
      header = receive_message_on_socket(16, sock)
      size, request_id, response_to = header.unpack('VVV')
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

    # Constructs a getlasterror message. This method is used exclusively by
    # Connection#send_message_with_safe_check.
    #
    # Because it modifies message by reference, we don't need to return it.
    def build_last_error_message(message, db_name, opts)
      message.put_int(0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{db_name}.$cmd")
      message.put_int(0)
      message.put_int(-1)
      cmd = BSON::OrderedHash.new
      cmd[:getlasterror] = 1
      if opts.is_a?(Hash)
        opts.assert_valid_keys(:w, :wtimeout, :fsync, :j)
        cmd.merge!(opts)
      end
      message.put_binary(BSON::BSON_CODER.serialize(cmd, false).to_s)
      nil
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
        raise ConnectionFailure, "Operation failed with the following exception: #{ex}:#{ex.message}"
      end
    end

    # Low-level method for receiving data from socket.
    # Requires length and an available socket.
    def receive_message_on_socket(length, socket)
      begin
        if @op_timeout
          message = nil
          Mongo::TimeoutHandler.timeout(@op_timeout, OperationTimeout) do
            message = receive_data(length, socket)
          end
        else
          message = receive_data(length, socket)
        end
        rescue => ex
          close

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
