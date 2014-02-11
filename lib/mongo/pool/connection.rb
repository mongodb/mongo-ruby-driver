# Copyright (C) 2009-2014 MongoDB, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Pool

    # This class models the database connections and their behavior.
    class Connection

      TIMEOUT = 5

      attr_reader :host, :port, :timeout

      def initialize(host, port, timeout = nil, options = {})
        @host     = host
        @port     = port
        @timeout  = timeout || TIMEOUT
        @socket   = nil
        @ssl_opts = options.reject { |k, v| !k.to_s.start_with?('ssl') }
      end

      def connect
        # if host && port.nil?
          # @socket = Socket::Unix.new(host, timeout)
        # else
          # if ssl_opts && !ssl_opts.empty?
            # socket = Socket::SSL.new(host, port, timeout, ssl_opts)
          # else
            # socket = Socket::TCP.new(host, port, timeout)
          # end
        # end
      end

      def disconnect
        if @socket
          @socket.close
          @socket = nil
        end
      end

      def read
        # Protocol::Reply.deserialize(@socket).documents
      end

      def write(message)
        # @socket.write(message.serialize)
      end

      private

      attr_reader :socket, :ssl_opts
    end
  end
end
