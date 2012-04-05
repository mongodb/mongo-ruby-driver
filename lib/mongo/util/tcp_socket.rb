require 'socket'

module Mongo
  # Wrapper class for Socket
  #
  # Emulates TCPSocket with operation and connection timeout
  # sans Timeout::timeout
  #
  class TCPSocket
    attr_accessor :pool

    def initialize(host, port, op_timeout=nil, connect_timeout=nil)
      @op_timeout = op_timeout 
      @connect_timeout = connect_timeout

      # TODO: Prefer ipv6 if server is ipv6 enabled
      @host = Socket.getaddrinfo(host, nil, Socket::AF_INET).first[3]
      @port = port
      @socket_address = Socket.pack_sockaddr_in(@port, @host)
      @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)

      connect
    end

    def connect
      # Connect nonblock is broken in current versions of JRuby
      if RUBY_PLATFORM == 'java'
        require 'timeout'
        if @connect_timeout
          Timeout::timeout(@connect_timeout, OperationTimeout) do
            @socket.connect(@socket_address)
          end
        else
          @socket.connect(@socket_address)
        end
      else
        # Try to connect for @connect_timeout seconds
        begin
          @socket.connect_nonblock(@socket_address)
        rescue Errno::EINPROGRESS
          # Block until there is a response or error
          resp = IO.select([@socket], [@socket], [@socket], @connect_timeout)
          if resp.nil?
            raise ConnectionTimeoutError
          end
        end

        # If there was a failure this will raise an Error
        begin
          @socket.connect_nonblock(@socket_address)
        rescue Errno::EISCONN
          # Successfully connected
        end
      end
    end

    def send(data)
      @socket.write(data)
    end

    def read(maxlen, buffer)
      # Block on data to read for @op_timeout seconds
      begin
        ready = IO.select([@socket], nil, [@socket], @op_timeout)
      rescue IOError
        raise OperationFailure
      end
      if ready
        begin
          @socket.readpartial(maxlen, buffer)
        rescue EOFError
          return ConnectionError
        rescue Errno::ENOTCONN, Errno::EBADF, Errno::ECONNRESET, Errno::EPIPE
          raise ConnectionFailure
        rescue Errno::EINTR, Errno::EIO, IOError 
          raise OperationFailure 
        end
      else
        raise OperationTimeout
      end
    end

    def setsockopt(key, value, n)
      @socket.setsockopt(key, value, n)
    end

    def close
      @socket.close
    end

    def closed?
      @socket.closed?
    end
  end
end
