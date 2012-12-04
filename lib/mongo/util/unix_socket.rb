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
      @host = socket_path # TODO rename instance var
      @port = port
      @socket_address = Socket.sockaddr_un(@host)
      @socket = Socket.new(Socket::AF_UNIX, Socket::SOCK_STREAM, 0)
      connect
    end
  end
end

