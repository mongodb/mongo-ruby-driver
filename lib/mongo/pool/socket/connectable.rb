# Copyright (C) 2009-2014 MongoDB, Inc.
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
  class Pool
    module Socket

      # Module for behavior common across all supported socket types.
      module Connectable
        include ::Socket::Constants

        # Error message for timeouts on socket calls.
        #
        # @since 3.0.0
        TIMEOUT_ERROR = 'Socket request timed out'.freeze

        # The pack directive for timeouts.
        #
        # @since 3.0.0
        TIMEOUT_PACK = 'l_2'.freeze

        # Error message for SSL related exceptions.
        #
        # @since 3.0.0
        SSL_ERROR = 'SSL handshake failed. MongoDB may not be configured with SSL support.'.freeze

        # @return [ Integer ] family The socket family (IPv4, IPv6, Unix).
        attr_reader :family

        # @return [ String ] host The host to connect to.
        attr_reader :host

        # @return [ Integer ] port The port to connect to.
        attr_reader :port

        # @return [ Float ] timeout The connection timeout.
        attr_reader :timeout

        # Determine if the socket is alive.
        #
        # @example Is the socket alive?
        #   socket.alive?
        #
        # @return [ true, false ] If the socket is alive.
        #
        # @since 3.0.0
        def alive?
          begin
            Kernel::select([ @socket ], nil, [ @socket ], 0) ? !eof? : true
          rescue
            false
          end
        end

        # Close the wrapped socket.
        #
        # @example Close the wrapped socket.
        #   socket.close
        #
        # @return [ true ] True if the socket completed closing.
        #
        # @since 3.0.0
        def close
          @socket.close and true
        end

        # Will read all data from the socket for the provided number of bytes.
        # If less data is returned than requested, an exception will be raised.
        #
        # @example Read all the requested data from the socket.
        #   socket.read(4096)
        #
        # @param [ Integer ] length The number of bytes to read.
        #
        # @raise [ Mongo::SocketError ] If not all data is returned.
        #
        # @return [ Object ] The data from the socket.
        #
        # @since 3.0.0
        def read(length)
          data = handle_errors { @socket.read(length) }
          unless data
            raise SocketError, "Attempted to read #{length} bytes from the socket but got none."
          end
          data << read_all(length - data.length) if data.length < length
          data
        end

        # Delegates gets to the underlying socket.
        #
        # @example Get the next line.
        #   socket.gets(10)
        #
        # @param [ Array<Object> ] args The arguments to pass through.
        #
        # @return [ Object ] The returned bytes.
        #
        # @since 3.0.0
        def gets(*args)
          handle_errors { @socket.gets(*args) }
        end

        # Read a single byte from the socket.
        #
        # @example Read a single byte.
        #   socket.readbyte
        #
        # @return [ Object ] The read byte.
        #
        # @since 3.0.0
        def readbyte
          handle_errors { @socket.readbyte }
        end

        # Writes data to the socket instance.
        #
        # @example Write to the socket.
        #   socket.write(data)
        #
        # @param [ Array<Object> ] args The data to be written.
        #
        # @return [ Integer ] The length of bytes written to the socket.
        #
        # @since 3.0.0
        def write(*args)
          handle_errors { @socket.write(*args) }
        end

        private

        def default_socket
          sock = ::Socket.new(family, SOCK_STREAM, 0)
          sock.set_encoding(BSON::BINARY)
          sock.setsockopt(SOL_SOCKET, SO_RCVTIMEO, encoded_timeout)
          sock.setsockopt(SOL_SOCKET, SO_SNDTIMEO, encoded_timeout)
          sock
        end

        def encoded_timeout
          @encoded_timeout ||= [ timeout, 0 ].pack(TIMEOUT_PACK)
        end

        def handle_errors
          yield
          rescue Errno::ETIMEDOUT
            raise Mongo::SocketTimeoutError, TIMEOUT_ERROR
          rescue IOError, SystemCallError => e
            raise Mongo::SocketError, e.message
          rescue OpenSSL::SSL::SSLError
            raise Mongo::SocketError, SSL_ERROR
        end

        def initialize_socket
          sock = default_socket
          sock.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
          sock.connect(::Socket.pack_sockaddr_in(port, host))
          sock
        end
      end
    end
  end
end
