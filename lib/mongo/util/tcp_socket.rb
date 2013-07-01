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
require 'timeout'

module Mongo
  # Wrapper class for Socket
  #
  # Emulates TCPSocket with operation and connection timeout
  # sans Timeout::timeout
  #
  class TCPSocket
    include SocketUtil

    def initialize(host, port, op_timeout=nil, connect_timeout=nil, opts={})
      @op_timeout      = op_timeout
      @connect_timeout = connect_timeout
      @pid             = Process.pid

      # TODO: Prefer ipv6 if server is ipv6 enabled
      @address = Socket.getaddrinfo(host, nil, Socket::AF_INET).first[3]
      @port    = port

      @socket_address = Socket.pack_sockaddr_in(@port, @address)
      @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      connect
    end

    def connect
      if @connect_timeout
        Timeout::timeout(@connect_timeout, ConnectionTimeoutError) do
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
