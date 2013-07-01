# Copyright (C) 2013 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'socket'
require 'openssl'
require 'timeout'

module Mongo

  # A basic wrapper over Ruby's SSLSocket that initiates
  # a TCP connection over SSL and then provides an basic interface
  # mirroring Ruby's TCPSocket, vis., TCPSocket#send and TCPSocket#read.
  class SSLSocket
    include SocketUtil

    def initialize(host, port, op_timeout=nil, connect_timeout=nil, opts={})
      @pid             = Process.pid
      @op_timeout      = op_timeout
      @connect_timeout = connect_timeout

      @tcp_socket = ::TCPSocket.new(host, port)
      @tcp_socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      @context = OpenSSL::SSL::SSLContext.new

      if opts[:cert]
        @context.cert = OpenSSL::X509::Certificate.new(File.open(opts[:cert]))
      end

      if opts[:key]
        @context.key = OpenSSL::PKey::RSA.new(File.open(opts[:key]))
      end

      if opts[:verify]
        @context.ca_file = opts[:ca_cert]
        @context.verify_mode = OpenSSL::SSL::VERIFY_PEER
      end

      begin
        @socket = OpenSSL::SSL::SSLSocket.new(@tcp_socket, @context)
        @socket.sync_close = true
        connect
      rescue OpenSSL::SSL::SSLError
        raise ConnectionFailure, "SSL handshake failed. MongoDB may " +
                                 "not be configured with SSL support."
      end

      if opts[:verify]
        unless OpenSSL::SSL.verify_certificate_identity(@socket.peer_cert, host)
          raise ConnectionFailure, "SSL handshake failed. Hostname mismatch."
        end
      end

      self
    end

    def connect
      if @connect_timeout
        Timeout::timeout(@connect_timeout, ConnectionTimeoutError) do
          @socket.connect
        end
      else
        @socket.connect
      end
    end

    def send(data)
      @socket.syswrite(data)
    end

    def read(length, buffer)
      if @op_timeout
        Timeout::timeout(@op_timeout, OperationTimeout) do
          @socket.sysread(length, buffer)
        end
      else
        @socket.sysread(length, buffer)
      end
    end
  end
end
