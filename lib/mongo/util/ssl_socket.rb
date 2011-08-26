require 'openssl'

module Mongo

  # A basic wrapper over Ruby's SSLSocket that initiates
  # a TCP connection over SSL and then provides an basic interface
  # mirroring Ruby's TCPSocket, vis., TCPSocket#send and TCPSocket#read.
  class SSLSocket

    def initialize(host, port)
      @socket = ::TCPSocket.new(host, port)
      @ssl = OpenSSL::SSL::SSLSocket.new(@socket)
      @ssl.sync_close = true
      @ssl.connect
    end

    def setsockopt(key, value, n)
      @socket.setsockopt(key, value, n)
    end

    # Write to the SSL socket.
    #
    # @param buffer a buffer to send.
    # @param flags socket flags. Because Ruby's SSL
    def send(buffer, flags=0)
      @ssl.syswrite(buffer)
    end

    def read(length, buffer)
      @ssl.sysread(length, buffer)
    end

    def close
      @ssl.close
    end

  end
end
