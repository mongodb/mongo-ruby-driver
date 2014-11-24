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

    # A FIFO queue of connections to be used by the connection pool. This is
    # based on mperham's connection pool, implemented with a queue instead of a
    # stack.
    #
    # @since 2.0.0
    class Queue

      # The default max size for the queue.
      MAX_SIZE = 5.freeze

      # The default min size for the queue.
      MIN_SIZE = 1.freeze

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
      #   queue.dequeue(1.0)
      #
      # @param [ Float ] timeout The time to wait in seconds.
      #
      # @return [ Mongo::Pool::Connection ] The next connection.
      #
      # @since 2.0.0
      def dequeue(timeout = 0.5)
        mutex.synchronize do
          dequeue_connection(timeout)
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
          queue.push(connection)
          resource.broadcast
        end
      end

      # Initialize the new queue. Will yield the block the number of times for
      # the initial size of the queue.
      #
      # @example Create the queue.
      #   Mongo::Pool::Queue.new(5)
      #
      # @param [ Integer ] size The initial size of the queue.
      #
      # @since 2.0.0
      def initialize(options = {}, &block)
        @block = block
        @connections = 0
        @options = options
        @queue = []
        @mutex = Mutex.new
        @resource = ConditionVariable.new
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

      private

      def dequeue_connection(timeout)
        deadline = Time.now - timeout
        loop do
          return queue.delete_at(0) unless queue.empty?
          connection = create_connection
          return connection if connection
          wait_for_next!(deadline, timeout)
        end
      end

      def create_connection
        if @connections < max_size
          @connections += 1
          @block.call
        end
      end

      def wait_for_next!(deadline, timeout)
        wait = deadline - Time.now
        if wait <= 0
          raise Timeout::Error.new("Timed out attempting to dequeue connection after #{timeout} sec.")
        end
        resource.wait(mutex, wait)
      end
    end
  end
end
