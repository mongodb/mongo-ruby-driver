require 'socket'
require 'timeout'

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
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      connect
    end

    def connect
      if @connect_timeout
        Timeout::timeout(@connect_timeout, OperationTimeout) do
          @socket.connect(@socket_address)
        end
      else
        @socket.connect(@socket_address)
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
          @socket.sysread(maxlen, buffer)
        rescue SystemCallError => ex
          # Needed because sometimes JRUBY doesn't throw Errno::ECONNRESET as it should
          # http://jira.codehaus.org/browse/JRUBY-6180
          raise ConnectionFailure, ex
        rescue Errno::ENOTCONN, Errno::EBADF, Errno::ECONNRESET, Errno::EPIPE, Errno::ETIMEDOUT, EOFError => ex
          raise ConnectionFailure, ex
        rescue Errno::EINTR, Errno::EIO, IOError => ex
          raise OperationFailure, ex
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
