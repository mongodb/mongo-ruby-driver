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

    def initialize(host, port, op_timeout=nil, connect_timeout=nil)
      @op_timeout = op_timeout
      @connect_timeout = connect_timeout
      @pid = Process.pid

      @tcp_socket = ::TCPSocket.new(host, port)
      @tcp_socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      @socket = OpenSSL::SSL::SSLSocket.new(@tcp_socket)
      @socket.sync_close = true

      connect
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
