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

module Mongo
  class Pool
    module Socket

      # Wrapper for TCP sockets.
      #
      # @since 3.0.0
      class TCP
        include Socket::Base

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
        # @since 3.0.0
        def connect!
          Timeout.timeout(timeout, Mongo::SocketTimeoutError) do
            @socket = handle_connect
            self
          end
        end

        # Initializes a new TCP socket.
        #
        # @example Create the TCP socket.
        #   TCP.new('::1', 27017, 30)
        #   TCP.new('127.0.0.1', 27017, 30)
        #
        # @param host [ String ] The hostname or IP address.
        # @param port [ Integer ] The port number.
        # @param timeout [ Integer ] The socket timeout value.
        #
        # @since 3.0.0
        def initialize(host, port, timeout)
          @host    = host
          @port    = port
          @timeout = timeout
        end
      end
    end
  end
end
