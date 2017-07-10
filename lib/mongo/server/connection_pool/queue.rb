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

module Mongo
  class Server
    class ConnectionPool

      # A FIFO queue of connections to be used by the connection pool. This is
      # based on mperham's connection pool, implemented with a queue instead of a
      # stack.
      #
      # @since 2.0.0
      class Queue
        extend Forwardable

        # Size of the queue delegates to the wrapped queue.
        def_delegators :queue, :size

        # The default max size for the queue.
        MAX_SIZE = 5.freeze

        # The default min size for the queue.
        MIN_SIZE = 1.freeze

        # The default timeout, in seconds, to wait for a connection.
        WAIT_TIMEOUT = 1.freeze

        # @return [ Array ] queue The underlying array of connections.
        attr_reader :queue

        # @return [ Mutex ] mutex The mutex used for synchronization.
        attr_reader :mutex

        # @return [ Hash ] options The options.
        attr_reader :options

        # @return [ ConditionVariable ] resource The resource.
        attr_reader :resource

        # Dequeue a connection from the queue, waiting for the provided timeout
        # for an item if none is in the queue.
        #
        # @example Dequeue a connection.
        #   queue.dequeue
        #
        # @return [ Mongo::Pool::Connection ] The next connection.
        #
        # @since 2.0.0
        def dequeue
          mutex.synchronize do
            dequeue_connection
          end
        end

        # Disconnect all connections in the queue.
        #
        # @example Disconnect all connections.
        #   queue.disconnect!
        #
        # @return [ true ] Always true.
        #
        # @since 2.1.0
        def disconnect!
          mutex.synchronize do
            queue.each{ |connection| connection.disconnect! }
            true
          end
        end

        # Enqueue a connection in the queue.
        #
        # @example Enqueue a connection.
        #   queue.enqueue(connection)
        #
        # @param [ Mongo::Pool::Connection ] connection The connection.
        #
        # @since 2.0.0
        def enqueue(connection)
          mutex.synchronize do
            queue.unshift(connection)
            resource.broadcast
          end
        end

        # Initialize the new queue. Will yield the block the number of times for
        # the initial size of the queue.
        #
        # @example Create the queue.
        #   Mongo::Pool::Queue.new(max_pool_size: 5) { Connection.new }
        #
        # @param [ Hash ] options The options.
        #
        # @option options [ Integer ] :max_pool_size The maximum size.
        # @option options [ Integer ] :min_pool_size The minimum size.
        # @option options [ Float ] :wait_queue_timeout The time to wait, in
        #   seconds, for a free connection.
        #
        # @since 2.0.0
        def initialize(options = {}, &block)
          @block = block
          @connections = 0
          @options = options
          @queue = Array.new(min_size) { create_connection }
          @mutex = Mutex.new
          @resource = ConditionVariable.new
        end

        # Get a pretty printed string inspection for the queue.
        #
        # @example Inspect the queue.
        #   queue.inspect
        #
        # @return [ String ] The queue inspection.
        #
        # @since 2.0.0
        def inspect
          "#<Mongo::Pool::Queue:0x#{object_id} min_size=#{min_size} max_size=#{max_size} " +
            "wait_timeout=#{wait_timeout} current_size=#{queue.size}>"
        end

        # Get the maximum size of the queue.
        #
        # @example Get the max size.
        #   queue.max_size
        #
        # @return [ Integer ] The maximum size of the queue.
        #
        # @since 2.0.0
        def max_size
          @max_size ||= options[:max_pool_size] || MAX_SIZE
        end

        # Get the minimum size of the queue.
        #
        # @example Get the min size.
        #   queue.min_size
        #
        # @return [ Integer ] The minimum size of the queue.
        #
        # @since 2.0.0
        def min_size
          @min_size ||= options[:min_pool_size] || MIN_SIZE
        end

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

        private

        def dequeue_connection
          deadline = Time.now + wait_timeout
          loop do
            return queue.pop unless queue.empty?
            connection = create_connection
            return connection if connection
            wait_for_next!(deadline)
          end
        end

        def create_connection
          if @connections < max_size
            @connections += 1
            @block.call
          end
        end

        def wait_for_next!(deadline)
          wait = deadline - Time.now
          if wait <= 0
            raise Timeout::Error.new("Timed out attempting to dequeue connection after #{wait_timeout} sec.")
          end
          resource.wait(mutex, wait)
        end
      end
    end
  end
end
