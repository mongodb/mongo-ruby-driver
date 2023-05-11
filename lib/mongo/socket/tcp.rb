# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
      # @param [ Hash ] options The options.
      #
      # @option options [ Float ] :connect_timeout Connect timeout.
      # @option options [ Address ] :connection_address Address of the
      #   connection that created this socket.
      # @option options [ Integer ] :connection_generation Generation of the
      #   connection (for non-monitoring connections) that created this socket.
      # @option options [ true | false ] :monitor Whether this socket was
      #   created by a monitoring connection.
      #
      # @since 2.0.0
      # @api private
      def initialize(host, port, timeout, family, options = {})
        if family.nil?
          raise ArgumentError, 'family must be specified'
        end
        super(timeout, options)
        @host, @port = host, port
        @family = family
        @socket = ::Socket.new(family, SOCK_STREAM, 0)
        begin
          set_socket_options(@socket)
          connect!
        rescue
          @socket.close
          raise
        end
      end

      # @return [ String ] host The host to connect to.
      attr_reader :host

      # @return [ Integer ] port The port to connect to.
      attr_reader :port

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
      def connect!
        Timeout.timeout(options[:connect_timeout], Error::SocketTimeoutError, "The socket took over #{options[:connect_timeout]} seconds to connect") do
          socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
          map_exceptions do
            socket.connect(::Socket.pack_sockaddr_in(port, host))
          end
          self
        end
      end
      private :connect!

      private

      def human_address
        "#{host}:#{port} (no TLS)"
      end
    end
  end
end
