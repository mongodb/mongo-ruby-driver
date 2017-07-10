# Copyright (C) 2014-2017 MongoDB, Inc.
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

require 'mongo/server/connection_pool/queue'

module Mongo
  class Server

    # Represents a connection pool for server connections.
    #
    # @since 2.0.0
    class ConnectionPool
      include Loggable

      # @return [ Hash ] options The pool options.
      attr_reader :options

      # Check a connection back into the pool. Will pull the connection from a
      # thread local stack that should contain it after it was checked out.
      #
      # @example Checkin the thread's connection to the pool.
      #   pool.checkin
      #
      # @since 2.0.0
      def checkin(connection)
        queue.enqueue(connection)
      end

      # Check a connection out from the pool. If a connection exists on the same
      # thread it will get that connection, otherwise it will dequeue a
      # connection from the queue and pin it to this thread.
      #
      # @example Check a connection out from the pool.
      #   pool.checkout
      #
      # @return [ Mongo::Pool::Connection ] The checked out connection.
      #
      # @since 2.0.0
      def checkout
        queue.dequeue
      end

      # Disconnect the connection pool.
      #
      # @example Disconnect the connection pool.
      #   pool.disconnect!
      #
      # @return [ true ] true.
      #
      # @since 2.1.0
      def disconnect!
        queue.disconnect!
      end

      # Create the new connection pool.
      #
      # @example Create the new connection pool.
      #   Pool.new(timeout: 0.5) do
      #     Connection.new
      #   end
      #
      # @note A block must be passed to set up the connections on initialization.
      #
      # @param [ Hash ] options The connection pool options.
      #
      # @since 2.0.0
      def initialize(options = {}, &block)
        @options = options.freeze
        @queue = Queue.new(options, &block)
      end

      # Get a pretty printed string inspection for the pool.
      #
      # @example Inspect the pool.
      #   pool.inspect
      #
      # @return [ String ] The pool inspection.
      #
      # @since 2.0.0
      def inspect
        "#<Mongo::Server::ConnectionPool:0x#{object_id} queue=#{queue.inspect}>"
      end

      # Yield the block to a connection, while handling checkin/checkout logic.
      #
      # @example Execute with a connection.
      #   pool.with_connection do |connection|
      #     connection.read
      #   end
      #
      # @return [ Object ] The result of the block.
      #
      # @since 2.0.0
      def with_connection
        connection = checkout
        yield(connection)
      ensure
        checkin(connection) if connection
      end

      protected

      attr_reader :queue

      private

      class << self

        # Get a connection pool for the provided server.
        #
        # @example Get a connection pool.
        #   Mongo::Pool.get(server)
        #
        # @param [ Mongo::Server ] server The server.
        #
        # @return [ Mongo::Pool ] The connection pool.
        #
        # @since 2.0.0
        def get(server)
          ConnectionPool.new(server.options) do
            Connection.new(server, server.options)
          end
        end
      end
    end
  end
end
