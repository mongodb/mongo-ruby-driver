# Copyright (C) 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Server

    # Represents a context in which messages are sent to the server on a
    # connection.
    #
    # @since 2.0.0
    class Context
      extend Forwardable

      # @return [ Mongo::Server ] server The server the context is for.
      attr_reader :server

      # Delegate state checks to the server.
      def_delegators :@server,
                     :cluster,
                     :features,
                     :max_wire_version,
                     :max_write_batch_size,
                     :mongos?,
                     :primary?,
                     :secondary?,
                     :standalone?

      # Instantiate a server context.
      #
      # @example Instantiate a server context.
      #   Mongo::Server::Context.new(server)
      #
      # @param [ Mongo::Server ] server The server the context is for.
      #
      # @since 2.0.0
      def initialize(server)
        @server = server
      end

      # Execute a block of code with a connection, that is checked out of the
      # pool and then checked back in.
      #
      # @example Send a message with the connection.
      #   context.with_connection do |connection|
      #     connection.dispatch([ command ])
      #   end
      #
      # @return [ Object ] The result of the block execution.
      #
      # @since 2.0.0
      def with_connection(&block)
        server.pool.with_connection(&block)
      end
    end
  end
end
