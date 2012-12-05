require 'socket'

module Mongo
  # Wrapper class for Socket
  #
  # Emulates UNIXSocket with operation and connection timeout
  # sans Timeout::timeout
  #
  class UNIXSocket < TCPSocket
    def initialize(socket_path, port=:socket, op_timeout=nil, connect_timeout=nil)
      @op_timeout = op_timeout
      @connect_timeout = connect_timeout

      @address = socket_path
      @port = :socket # purposely override input

      @socket_address = Socket.pack_sockaddr_un(@address)
      @socket = Socket.new(Socket::AF_UNIX, Socket::SOCK_STREAM, 0)
      connect
    end
  end
end

