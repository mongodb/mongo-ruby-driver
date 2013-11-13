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

require 'socket'
require 'openssl'
require 'timeout'

module Mongo
  module Pool
    module Socket

      # Module for behavior common across all supported socket types.
      module Base

        include ::Socket::Constants

        # Reads data from the socket instance.
        #
        # @example
        #   socket.read(4096)
        #
        # @param  length [Integer] The length of data to read.
        #
        # @return [Object] The data read from the socket.
        def read(length)
          handle_socket_error { @socket.read(length) }
        end

        # Writes data to the socket instance.
        #
        # @example
        #   socket.write(data)
        #
        # @param  *args [Object] The data to be written.
        #
        # @return [Integer] The length of bytes written to the socket.
        def write(*args)
          handle_socket_error { @socket.write(args) }
        end

        private

        # Helper method to handle connection logic for tcp socket types and
        # all possible socket address families.
        #
        # @api private
        #
        # @example
        #   handle_connect
        #
        # @return [Socket] The connected socket instance.
        def handle_connect
          error  = nil
          addr_info = ::Socket.getaddrinfo(@host, nil, AF_UNSPEC, SOCK_STREAM)
          addr_info.each do |info|
            begin
              sock        = create_socket(info[4])
              socket_addr = ::Socket.pack_sockaddr_in(@port, info[3])
              sock.connect(socket_addr)
              return sock
            rescue IOError, SystemCallError => e
              error = e
            end
          end
          raise error
        end

        # Initializes a new socket instance with default options and encoding.
        #
        # @api private
        #
        # @example
        #   create_socket(Socket::AF_INET)
        #   create_socket(Socket::AF_INET6)
        #   create_socket(Socket::AF_UNIX)
        #
        # @param family [Integer] The socket address family.
        #
        # @return [Socket] The newly created socket instance.
        def create_socket(family)
          sock = ::Socket.new(family, SOCK_STREAM, 0)
          sock.set_encoding('binary') if sock.respond_to?(:set_encoding)
          sock.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1) if family != AF_UNIX

          timeout_value = [@timeout, 0].pack('l_2')
          sock.setsockopt(SOL_SOCKET, SO_RCVTIMEO, timeout_value)
          sock.setsockopt(SOL_SOCKET, SO_SNDTIMEO, timeout_value)

          sock
        end

        # Utility method of handing socket exceptions, generating an
        # appropriate error message and raising them as a Mongo::Error
        # exception.
        #
        # @api private
        #
        # @example
        #   handle_socket_error do
        #     socket.write(payload)
        #   end
        #
        # @return [Object] The yield result.
        def handle_socket_error
          yield
          rescue Errno::ETIMEDOUT
            raise Mongo::SocketTimeoutError,
                  'Socket request timed out.'
          rescue IOError, SystemCallError
            raise Mongo::SocketError,
                  'A socket error occurred.'
          rescue OpenSSL::SSL::SSLError
            raise Mongo::SocketError,
                  'SSL handshake failed. MongoDB ' +
                  'may not be configured with SSL support.'
        end

      end

    end
  end
end
