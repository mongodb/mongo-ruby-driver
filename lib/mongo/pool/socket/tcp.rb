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

      # Wrapper for TCP sockets.
      class TCP

        include Socket::Base

        # Initializes a new TCP socket.
        #
        # @example
        #   TCP.new('::1', 27017, 30)
        #   TCP.new('127.0.0.1', 27017, 30)
        #   TCP.new('127.0.0.1', 27017, 30, :connect => false)
        #
        # @param host [String] The hostname or IP address.
        # @param port [Integer] The port number.
        # @param timeout [Integer] The socket timeout value.
        # @param opts [Hash] Optional settings and configuration values.
        #
        # @option opts [true, false] :connect (true) If true calls connect
        #   before returning the object instance.
        #
        # @return [TCP] The TCP socket instance.
        def initialize(host, port, timeout, opts = {})
          @host    = host
          @port    = port
          @timeout = timeout

          connect if opts.fetch(:connect, true)
          self
        end

        # Establishes a socket connection.
        #
        # @example
        #   sock = TCP.new('::1', 27017, 30)
        #   sock.connect
        #
        # @return [Socket] The connected socket instance.
        def connect
          Timeout.timeout(@timeout, Mongo::SocketTimeoutError) do
            @socket = handle_connect
          end
        end

      end

    end
  end
end
