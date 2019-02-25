# Copyright (C) 2014-2019 MongoDB, Inc.
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
      extend Forwardable

      # Create the new connection pool.
      #
      # @example Create the new connection pool.
      #   Pool.new(wait_queue_timeout: 0.5) do
      #     Connection.new
      #   end
      #
      # @note A block must be passed to set up the connections on initialization.
      #
      # @param [ Hash ] options The connection pool options.
      #
      # @option options [ Integer ] :max_pool_size The maximum pool size.
      # @option options [ Integer ] :min_pool_size The minimum pool size.
      # @option options [ Float ] :wait_queue_timeout The time to wait, in
      #   seconds, for a free connection.
      #
      # @since 2.0.0
      def initialize(options = {}, &block)
        @options = options.dup.freeze
        @queue = queue = Queue.new(@options, &block)

        finalizer = proc do
          queue.disconnect!
        end
        ObjectSpace.define_finalizer(self, finalizer)
      end

      # @return [ Hash ] options The pool options.
      attr_reader :options

      def_delegators :queue, :close_stale_sockets!

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
      # @return [ Mongo::Server::Connection ] The checked out connection.
      #
      # @since 2.0.0
      def checkout
        queue.dequeue
      end

      # Closes all idle connections in the pool and schedules currently checked
      # out connections to be closed when they are checked back into the pool.
      # The pool remains operational and can create new connections when
      # requested.
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
    end
  end
end
