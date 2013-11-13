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
  module Pool
    module Socket

      # Wrapper for Unix sockets.
      class Unix

        include Socket::Base

        # Initializes a new Unix socket.
        #
        # @example
        #   Unix.new('/path/to/socket.sock', 30)
        #   Unix.new('/path/to/socket.sock', 30, :connect => false)
        #
        # @param path [String] The path to the unix socket.
        # @param timeout [Integer] The socket timeout value.
        # @param opts [Hash] Optional settings and configuration values.
        #
        # @option opts [true, false] :connect (true) If true calls connect
        #   before returning the object instance.
        #
        # @return [Unix] The Unix socket instance.
        def initialize(path, timeout, opts = {})
          @host    = path
          @timeout = timeout

          connect if opts.fetch(:connect, true)
          self
        end

        # Establishes a socket connection.
        #
        # @example
        #   sock = Unix.new('/path/to/socket.sock', 30)
        #   sock.connect
        #
        # @return [Socket] The connected socket instance.
        def connect
          Timeout.timeout(@timeout, Mongo::SocketTimeoutError) do
            begin
              @socket = create_socket(AF_UNIX)
              @socket.connect(@host)
              return @socket
            rescue IOError, SystemCallError => e
              @socket.close if @socket
              raise e
            end
          end
        end

      end

    end
  end
end
