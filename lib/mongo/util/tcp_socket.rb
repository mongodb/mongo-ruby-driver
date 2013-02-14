require 'socket'
require 'timeout'

module Mongo
  # Wrapper class for Socket
  #
  # Emulates TCPSocket with operation and connection timeout
  # sans Timeout::timeout
  #
  class TCPSocket
    include SocketUtil

    def initialize(host, port, op_timeout=nil, connect_timeout=nil)
      @op_timeout = op_timeout
      @connect_timeout = connect_timeout
      @pid = Process.pid

      # TODO: Prefer ipv6 if server is ipv6 enabled
      @address = Socket.getaddrinfo(host, nil, Socket::AF_INET).first[3]
      @port = port

      @socket_address = Socket.pack_sockaddr_in(@port, @address)
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
        unless ready
          raise OperationTimeout
        end
      rescue IOError
        raise ConnectionFailure
      end

      # Read data from socket
      begin
        @socket.sysread(maxlen, buffer)
      rescue SystemCallError, IOError => ex
        raise ConnectionFailure, ex
      end
    end
  end
end
