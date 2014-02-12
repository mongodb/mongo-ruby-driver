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

      # Wrapper for Unix sockets.
      #
      # @since 3.0.0
      class Unix
        include Socket::Base

        # Establishes a socket connection.
        #
        # @example Connect to the socket.
        #   sock.connect
        #
        # @return [ Unix ] The connected socket instance.
        #
        # @since 3.0.0
        def connect!
          Timeout.timeout(timeout, Mongo::SocketTimeoutError) do
            begin
              @socket = create_socket(AF_UNIX)
              @socket.connect(host)
              self
            rescue IOError, SystemCallError => e
              @socket.close if @socket
              raise e
            end
          end
        end

        # Initializes a new Unix socket.
        #
        # @example Create the Unix socket.
        #   Unix.new('/path/to/socket.sock', 30)
        #
        # @param path [ String ] The path to the unix socket.
        # @param timeout [ Integer ] The socket timeout value.
        #
        # @since 3.0.0
        def initialize(path, timeout)
          @host    = path
          @timeout = timeout
        end
      end
    end
  end
end
