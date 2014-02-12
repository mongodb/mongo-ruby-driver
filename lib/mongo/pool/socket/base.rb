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
      module Base
        include ::Socket::Constants

        # @return [ String ] host The host to connect to.
        attr_reader :host

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

        # Reads data from the socket instance.
        #
        # @example Read from the socket.
        #   socket.read(4096)
        #
        # @param [ Integer ] length The length of data to read.
        #
        # @return [ Object ] The data read from the socket.
        #
        # @since 3.0.0
        def read(length)
          handle_socket_error { @socket.read(length) }
        end

        def gets(*args)
          handle_socket_error { @socket.gets(*args) }
        end

        def readbyte
          handle_socket_error { @socket.readbyte }
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
          handle_socket_error { @socket.write(*args) }
        end

        private

        def handle_connect
          error = nil
          addr_info = ::Socket.getaddrinfo(host, nil, AF_UNSPEC, SOCK_STREAM)
          addr_info.each do |info|
            begin
              sock        = create_socket(info[4])
              socket_addr = ::Socket.pack_sockaddr_in(port, info[3])
              sock.connect(socket_addr)
              return sock
            rescue IOError, SystemCallError => e
              error = e
            end
          end
          raise error
        end

        def create_socket(family)
          sock = ::Socket.new(family, SOCK_STREAM, 0)
          sock.set_encoding('binary') if sock.respond_to?(:set_encoding)
          sock.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1) if family != AF_UNIX
          timeout_value = [timeout, 0].pack('l_2')
          sock.setsockopt(SOL_SOCKET, SO_RCVTIMEO, timeout_value)
          sock.setsockopt(SOL_SOCKET, SO_SNDTIMEO, timeout_value)
          sock
        end

        def handle_socket_error
          yield
          rescue Errno::ETIMEDOUT
            raise Mongo::SocketTimeoutError, 'Socket request timed out.'
          rescue IOError, SystemCallError
            raise Mongo::SocketError, 'A socket error occurred.'
          rescue OpenSSL::SSL::SSLError
            raise Mongo::SocketError, 'SSL handshake failed. MongoDB may not be configured with SSL support.'
        end
      end
    end
  end
end
