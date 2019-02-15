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

require 'mongo/server/connection_pool/available_stack'
require 'mongo/server/connection_pool/wait_queue'

module Mongo
  class Server

    # Represents a connection pool for server connections.
    #
    # @since 2.0.0
    class ConnectionPool
      include Id
      include Loggable
      include Monitoring::Publishable
      extend Forwardable

      # The default timeout, in seconds, to wait for a connection.
      WAIT_TIMEOUT = 1.freeze

      # Create the new connection pool.
      #
      # @example Create the new connection pool.
      #   Pool.new(server, wait_queue_timeout: 0.5) do
      #     Connection.new
      #   end
      #
      # @param [ Mongo::Server ] server The server that the connections should connect to. The
      #   ConnectionPool will use the server's options where applicable.
      #
      # @note A block must be passed to set up the connections on initialization. #
      #
      # @since 2.0.0
      # @api private
      def initialize(server, &block)
        @id = ConnectionPool.next_id
        @address = server.address
        @monitoring = server.monitoring
        @options = server.options.dup.freeze

        publish_cmap_event(
          Monitoring::Event::Cmap::PoolCreated.new(address, options)
        )

        @closed = false
        @connections = AvailableStack.new(address, monitoring, options, &block)
        @wait_queue = WaitQueue.new(address)
        @pool_size = connections.pool_size
      end

      # @return [ String ] address The address the pool's connections will connect to.
      #
      # @since 2.8.0
      attr_reader :address

      # @return [ Hash ] options The pool options.
      attr_reader :options

      def_delegators :connections, :close_stale_sockets!

      # The time to wait, in seconds, for a connection to become available.
      #
      # @example Get the wait timeout.
      #   queue.wait_timeout
      #
      # @return [ Float ] The queue wait timeout.
      #
      # @since 2.0.0
      def wait_timeout
        @wait_timeout ||= options[:wait_queue_timeout] || WAIT_TIMEOUT
      end

      # Check a connection back into the pool. Will pull the connection from a
      # thread local stack that should contain it after it was checked out.
      #
      # @example Checkin the thread's connection to the pool.
      #   pool.checkin
      #
      # @since 2.0.0
      def checkin(connection)
        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckedIn.new(address, connection.id)
        )

        if closed?
          publish_cmap_event(
              Monitoring::Event::Cmap::ConnectionClosed.new(
                  Monitoring::Event::Cmap::ConnectionClosed::POOL_CLOSED,
                  address,
                  connection.id,
              ),
          )

          connection.disconnect!
        else
          connections.push(connection)
        end
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
        raise_if_closed!

        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckoutStarted.new(address)
        )

        deadline = Time.now + wait_timeout
        @wait_queue.enter_wait_queue(wait_timeout, deadline) { connections.pop(deadline) }.tap do |c|
            publish_cmap_event(
              Monitoring::Event::Cmap::ConnectionCheckedOut.new(address, c.id),
            )
        end
      rescue Error::ConnectionCheckoutTimeout
        publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionCheckoutFailed.new(
              Monitoring::Event::Cmap::ConnectionCheckoutFailed::TIMEOUT,
              address,
            ),
        )
        raise
      end

      # Updates the generation number. The connections will be disconnected and removed lazily
      # when the queue attempts to dequeue them.
      #
      # @return [ true ] true.
      #
      # @since 2.8.0
      def clear
        raise_if_closed!

        connections.clear

        publish_cmap_event(
          Monitoring::Event::Cmap::PoolCleared.new(address)
        )

        true
      end

      # Disconnects the pool and prevents any more connections from being checked out afterwards.
      # If #checkout is called after #close!, a Mongo::Error::PoolClosed error will be raised.
      #
      # @return [ true ] true.
      #
      # @since 2.8.0
      def close!
        return if closed?

        @closed = true
        @wait_queue.clear
        if connections
          connections.close!

          publish_cmap_event(
            Monitoring::Event::Cmap::PoolClosed.new(address)
          )
        end

        @connections = nil
        true
      end

      alias :disconnect! :close!

      # Get a pretty printed string inspection for the pool.
      #
      # @example Inspect the pool.
      #   pool.inspect
      #
      # @return [ String ] The pool inspection.
      #
      # @since 2.0.0
      def inspect
        "#<Mongo::Server::ConnectionPool:0x#{object_id} " +
          "queue=#{connections.inspect} wait_timeout=#{wait_timeout}>"
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
        raise_if_closed!

        connection = checkout
        yield(connection)
      ensure
        checkin(connection) if connection
      end

      protected

      attr_reader :connections

      private

      # Checks whether the pool has been closed.
      #
      # @return [ true | false ] Whether the pool is closed.
      #
      # @since 2.8.0
      def closed?
        @closed
      end

      # Asserts that the pool has not been closed.
      #
      # @raise [ Error::PoolClosed ] If the pool has been closed.
      #
      # @since 2.8.0
      def raise_if_closed!
        raise Error::PoolClosed.new(address, @pool_size) if closed?
      end

      class << self

        # Creates a new connection pool for the provided server.
        #
        # @example Create a new connection pool.
        #   Mongo::Server::ConnectionPool.get(server)
        #
        # @param [ Mongo::Server ] server The server.
        #
        # @return [ Mongo::Server::ConnectionPool ] The connection pool.
        #
        # @since 2.0.0
        def get(server)
          ConnectionPool.new(server) do |generation|
            Connection.new(server, server.options.merge(generation: generation)).tap do |c|
              c.publish_cmap_event(
                Monitoring::Event::Cmap::ConnectionCreated.new(server.address, c.id)
              )
            end
          end
        end
      end
    end
  end
end
