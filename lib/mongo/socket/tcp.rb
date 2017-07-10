# Copyright (C) 2014-2017 MongoDB, Inc.
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
  class Socket

    # Wrapper for TCP sockets.
    #
    # @since 2.0.0
    class TCP < Socket

      # @return [ String ] host The host to connect to.
      attr_reader :host

      # @return [ Integer ] port The port to connect to.
      attr_reader :port

      # @return [ Float ] timeout The socket timeout.
      attr_reader :timeout

      # Establishes a socket connection.
      #
      # @example Connect the socket.
      #   sock.connect!
      #
      # @note This method mutates the object by setting the socket
      #   internally.
      #
      # @return [ TCP ] The connected socket instance.
      #
      # @since 2.0.0
      def connect!(connect_timeout = nil)
        Timeout.timeout(connect_timeout, Error::SocketTimeoutError) do
          socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
          socket.setsockopt(SOL_SOCKET, SO_KEEPALIVE, true)
          handle_errors { socket.connect(::Socket.pack_sockaddr_in(port, host)) }
          self
        end
      end

      # Initializes a new TCP socket.
      #
      # @example Create the TCP socket.
      #   TCP.new('::1', 27017, 30, Socket::PF_INET)
      #   TCP.new('127.0.0.1', 27017, 30, Socket::PF_INET)
      #
      # @param [ String ] host The hostname or IP address.
      # @param [ Integer ] port The port number.
      # @param [ Float ] timeout The socket timeout value.
      # @param [ Integer ] family The socket family.
      #
      # @since 2.0.0
      def initialize(host, port, timeout, family)
        @host, @port, @timeout = host, port, timeout
        super(family)
      end

      # This object does not wrap another socket so it's always connectable.
      #
      # @example Is the socket connectable?
      #   socket.connectable?
      #
      # @return [ true, false ] If the socket is connectable.
      #
      # @since 2.2.5
      def connectable?
        true
      end
    end
  end
end
