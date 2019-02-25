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

module Mongo
  class Server
    class ConnectionPool

      # A LIFO queue of connections to be used by the connection pool. This is
      # based on mperham's connection pool.
      #
      # @note The queue contains active connections that are available for
      #   use. It does not track connections which are in use (checked out).
      #   It is easy to confuse the size of the connection pool (number of
      #   connections that are used plus number of connections that are
      #   available for use) and the size of the queue (number of connections
      #   that have already been created that are available for use).
      #   API documentation for this class states whether each size refers
      #   to the pool or to the queue size. Note that minimum and maximum
      #   sizes only make sense when talking about the connection pool,
      #   as the size of the queue of available connections is determined by
      #   the size constraints of the pool plus how many connections are
      #   currently checked out.
      #
      # @since 2.0.0
      class Queue
        include Loggable
        extend Forwardable

        # The default max size for the connection pool.
        MAX_SIZE = 5.freeze

        # The default min size for the connection pool.
        MIN_SIZE = 1.freeze

        # The default timeout, in seconds, to wait for a connection.
        WAIT_TIMEOUT = 1.freeze

        # Initialize the new queue. Will yield the block the number of times
        # equal to the initial connection pool size.
        #
        # @example Create the queue.
        #   Mongo::Server::ConnectionPool::Queue.new(max_pool_size: 5) { Connection.new }
        #
        # @param [ Hash ] options The options.
        #
        # @option options [ Integer ] :max_pool_size The maximum pool size.
        # @option options [ Integer ] :min_pool_size The minimum pool size.
        # @option options [ Float ] :wait_queue_timeout The time to wait, in
        #   seconds, for a free connection.
        #
        # @since 2.0.0
        def initialize(options = {}, &block)
          if options[:min_pool_size] && options[:max_pool_size] &&
            options[:min_pool_size] > options[:max_pool_size]
          then
            raise ArgumentError, "Cannot have min size > max size"
          end
          @block = block
          # This is the number of connections in the pool.
          # Includes available connections in the queue and the checked
          # out connections that we don't otherwise track.
          @pool_size = 0
          @options = options
          @generation = 1
          if min_size > max_size
            raise ArgumentError, "min_size (#{min_size}) cannot exceed max_size (#{max_size})"
          end
          @queue = Array.new(min_size) { create_connection }
          @mutex = Mutex.new
          @resource = ConditionVariable.new
          check_count_invariants
        end

        # @return [ Integer ] generation Generation of connections currently
        #   being used by the queue.
        #
        # @since 2.7.0
        # @api private
        attr_reader :generation

        # @return [ Array ] queue The underlying array of connections.
        attr_reader :queue

        # @return [ Mutex ] mutex The mutex used for synchronization of
        #   access to #queue.
        #
        # @api private
        attr_reader :mutex

        # @return [ Hash ] options The options.
        attr_reader :options

        # @return [ ConditionVariable ] resource The resource.
        attr_reader :resource

        # Number of connections that the pool has which are ready to be
        # checked out. This is NOT the size of the connection pool (total
        # number of active connections created by the pool).
        def size
          mutex.synchronize do
            queue.size
          end
        end

        # Number of connections that the pool has which are ready to be
        # checked out.
        #
        # @since 2.7.0
        alias_method :queue_size, :size

        # Number of connections in the pool (active connections ready to
        # be checked out plus connections already checked out).
        #
        # @since 2.7.0
        attr_reader :pool_size

        # Retrieves a connection. If there are active connections in the
        # queue, the most recently used connection is returned. Otherwise
        # if the connection pool size is less than the max size, creates a
        # new connection and returns it. Otherwise raises Timeout::Error.
        #
        # @example Dequeue a connection.
        #   queue.dequeue
        #
        # @return [ Mongo::Server::Connection ] The next connection.
        # @raise [ Timeout::Error ] If the connection pool is at maximum size
        #   and remains so for longer than the wait timeout.
        #
        # @since 2.0.0
        def dequeue
          check_count_invariants
          dequeue_connection
        ensure
          check_count_invariants
        end

        # Closes all idle connections in the queue and schedules currently
        # dequeued connections to be closed when they are enqueued back into
        # the queue. The queue remains operational and can create new
        # connections when requested.
        #
        # @example Disconnect all connections.
        #   queue.disconnect!
        #
        # @return [ true ] Always true.
        #
        # @since 2.1.0
        def disconnect!
          check_count_invariants
          mutex.synchronize do
            while connection = queue.pop
              connection.disconnect!
              @pool_size -= 1
              if @pool_size < 0
                # This should never happen
                log_warn("ConnectionPool::Queue: connection accounting problem")
                @pool_size = 0
              end
            end
            @generation += 1
            true
          end
        ensure
          check_count_invariants
        end

        # Enqueue a connection in the queue.
        #
        # Only connections created by this queue should be enqueued
        # back into it, however the queue does not verify whether it
        # originally created the connection being enqueued.
        #
        # If linting is enabled (see Mongo::Lint), attempting to enqueue
        # connections beyond the pool's capacity will raise Mongo::Error::LintError
        # (since some of those connections must not have originated from
        # the queue into which they are being enqueued). If linting is
        # not enabled, the queue can grow beyond its max size with undefined
        # results.
        #
        # @example Enqueue a connection.
        #   queue.enqueue(connection)
        #
        # @param [ Mongo::Server::Connection ] connection The connection.
        #
        # @since 2.0.0
        def enqueue(connection)
          check_count_invariants
          mutex.synchronize do
            if connection.generation == @generation
              queue.unshift(connection.record_checkin!)
              resource.broadcast
            else
              connection.disconnect!

              @pool_size = if @pool_size > 0
                @pool_size - 1
              else
                # This should never happen
                log_warn("ConnectionPool::Queue: unexpected enqueue")
                0
              end

              while @pool_size < min_size
                @pool_size += 1
                queue.unshift(@block.call(@generation))
              end
            end
          end
          nil
        ensure
          check_count_invariants
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
          "#<Mongo::Server::ConnectionPool::Queue:0x#{object_id} min_size=#{min_size} max_size=#{max_size} " +
            "wait_timeout=#{wait_timeout} current_size=#{queue_size}>"
        end

        # Get the maximum size of the connection pool.
        #
        # @example Get the max size.
        #   queue.max_size
        #
        # @return [ Integer ] The maximum size of the connection pool.
        #
        # @since 2.0.0
        def max_size
          @max_size ||= options[:max_pool_size] || [MAX_SIZE, min_size].max
        end

        # Get the minimum size of the connection pool.
        #
        # @example Get the min size.
        #   queue.min_size
        #
        # @return [ Integer ] The minimum size of the connection pool.
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

        # The maximum seconds a socket can remain idle since it has been
        # checked in to the pool.
        #
        # @example Get the max idle time.
        #   queue.max_idle_time
        #
        # @return [ Float ] The max socket idle time in seconds.
        #
        # @since 2.5.0
        def max_idle_time
          @max_idle_time ||= options[:max_idle_time]
        end

        # Close sockets that have been open for longer than the max idle time,
        #   if the option is set.
        #
        # @example Close the stale sockets
        #   queue.close_stale_sockets!
        #
        # @since 2.5.0
        def close_stale_sockets!
          check_count_invariants
          return unless max_idle_time

          mutex.synchronize do
            i = 0
            while i < queue.length
              connection = queue[i]
              if last_checkin = connection.last_checkin
                if (Time.now - last_checkin) > max_idle_time
                  connection.disconnect!
                  queue.delete_at(i)
                  @pool_size -= 1
                  next
                end
              end
              i += 1
            end
          end
        ensure
          check_count_invariants
        end

        private

        def dequeue_connection
          mutex.synchronize do
            deadline = Time.now + wait_timeout
            loop do
              return queue.shift unless queue.empty?
              connection = create_connection
              return connection if connection
              wait_for_next!(deadline)
            end
          end
        end

        def create_connection
          if pool_size < max_size
            @pool_size += 1
            @block.call(@generation)
          end
        end

        def wait_for_next!(deadline)
          wait = deadline - Time.now
          if wait <= 0
            raise Timeout::Error.new("Timed out attempting to dequeue connection after #{wait_timeout} sec.")
          end
          resource.wait(mutex, wait)
        end

        def check_count_invariants
          if Mongo::Lint.enabled?
            if pool_size < 0
              raise Error::LintError, 'connection pool queue: underflow'
            end
            if pool_size > max_size
              raise Error::LintError, 'connection pool queue: overflow'
            end
          end
        end
      end
    end
  end
end
