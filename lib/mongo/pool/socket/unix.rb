# Copyright (C) 2014-2015 MongoDB, Inc.
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
      # @since 2.0.0
      class Unix
        include Socket::Connectable

        # Establishes a socket connection.
        #
        # @example Connect to the socket.
        #   sock.connect
        #
        # @return [ Unix ] The connected socket instance.
        #
        # @since 2.0.0
        def connect!
          initialize!
        end

        # Initializes a new Unix socket.
        #
        # @example Create the Unix socket.
        #   Unix.new('/path/to/socket.sock', 30)
        #
        # @param [ String ] path The path to the unix socket.
        # @param [ Float ] timeout The socket timeout value.
        # @param [ Integer ] family The socket family.
        #
        # @since 2.0.0
        def initialize(path, timeout, family)
          @host    = path
          @timeout = timeout
          @family  = family
        end

        private

        def initialize_socket
          sock = default_socket
          sock.connect(host)
          sock
        end
      end
    end
  end
end
