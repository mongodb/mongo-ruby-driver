# Copyright (C) 2009-2013 MongoDB, Inc.
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
      @auths           = Set.new

      @socket = handle_connect(host, port)
    end

    def handle_connect(host, port)
      error = nil
      # Following python's lead (see PYTHON-356)
      family = host == 'localhost' ? Socket::AF_INET : Socket::AF_UNSPEC
      addr_info = Socket.getaddrinfo(host, nil, family, Socket::SOCK_STREAM)
      addr_info.each do |info|
        begin
          sock = Socket.new(info[4], Socket::SOCK_STREAM, 0)
          sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
          socket_address = Socket.pack_sockaddr_in(port, info[3])
          connect(sock, socket_address)
          return sock
        rescue IOError, SystemCallError => e
          error = e
          sock.close if sock
        end
      end
      raise error
    end

    def connect(socket, socket_address)
      if @connect_timeout
        Timeout::timeout(@connect_timeout, ConnectionTimeoutError) do
          socket.connect(socket_address)
        end
      else
        socket.connect(socket_address)
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
