require 'socket'
require 'openssl'
require 'timeout'

module Mongo

  # A basic wrapper over Ruby's SSLSocket that initiates
  # a TCP connection over SSL and then provides an basic interface
  # mirroring Ruby's TCPSocket, vis., TCPSocket#send and TCPSocket#read.
  class SSLSocket

    attr_accessor :pool, :pid

    def initialize(host, port, op_timeout=nil, connect_timeout=nil)
      @op_timeout = op_timeout
      @connect_timeout = connect_timeout
      @pid = Process.pid

      @socket = ::TCPSocket.new(host, port)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      @ssl = OpenSSL::SSL::SSLSocket.new(@socket)
      @ssl.sync_close = true

      connect
    end

    def connect
      if @connect_timeout
        Timeout::timeout(@connect_timeout, ConnectionTimeoutError) do
          @ssl.connect
        end
      else
        @ssl.connect
      end
    end

    def send(data)
      @ssl.syswrite(data)
    end

    def read(length, buffer)
      if @op_timeout
        Timeout::timeout(@op_timeout, OperationTimeout) do
          @ssl.sysread(length, buffer)
        end
      else
        @ssl.sysread(length, buffer)
      end 
    end

    def close
      @ssl.close
    end

    def closed?
      @ssl.closed?
    end
  end
end
