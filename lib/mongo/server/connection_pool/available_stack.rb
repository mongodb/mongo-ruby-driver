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

      # A LIFO stack of connections to be used by the connection pool. This is
      # based on mperham's connection pool.
      #
      # @note The stack contains active connections that are available for
      #   use. It does not track connections which are in use (checked out).
      #   It is easy to confuse the size of the connection pool (number of
      #   connections that are used plus number of connections that are
      #   available for use) and the size of the stack (number of connections
      #   that have already been created that are available for use).
      #   API documentation for this class states whether each size refers
      #   to the pool or to the stack size. Note that minimum and maximum
      #   sizes only make sense when talking about the connection pool,
      #   as the size of the stack of available connections is determined by
      #   the size constraints of the pool plus how many connections are
      #   currently checked out.
      #
      # @since 2.0.0
      class AvailableStack
        include Loggable
        include Monitoring::Publishable
        extend Forwardable

        # The default max size for the connection pool.
        MAX_SIZE = 5.freeze

        # The default min size for the connection pool.
        MIN_SIZE = 0.freeze

        # Initialize the new stack. Will yield the block the number of times
        # equal to the initial connection pool size.
        #
        # @example Create the stack.
        #   Mongo::Server::ConnectionPool::AvailableStack.new(address, monitoring, max_pool_size: 5) do
        #     Connection.new
        #   end
        #
        # @option options [ Integer ] :max_pool_size The maximum pool size.
        # @option options [ Integer ] :min_pool_size The minimum pool size.
        #   seconds, for a free connection.
        #
        # @since 2.0.0
        def initialize(address, monitoring, options = {}, &block)
          @address = address
          @monitoring = monitoring

          if options[:min_pool_size] && options[:max_pool_size] &&
            options[:min_pool_size] > options[:max_pool_size]
          then
            raise ArgumentError, "Cannot have min size > max size"
          end
          @block = block
          # This is the number of connections in the pool.
          # Includes available connections in the stack and the checked
          # out connections that we don't otherwise track.
          @pool_size = 0
          @options = options
          @generation = 1
          if min_size > max_size
            raise ArgumentError, "min_size (#{min_size}) cannot exceed max_size (#{max_size})"
          end
          @connections = Array.new(min_size) do
            create_connection.record_checkin!
          end

          @mutex = Mutex.new
          @connection_available_condvar = ConditionVariable.new

          check_count_invariants(true)
        end

        # @return [ Integer ] generation Generation of connections currently
        #   being used by the stack.
        #
        # @since 2.7.0
        # @api private
        attr_reader :generation

        # @return [ Array ] connections The underlying array of connections.
        attr_reader :connections

        # @return [ Mutex ] mutex The mutex used for synchronization.
        attr_reader :mutex

        # @return [ Hash ] options The options.
        attr_reader :options

        # @return [ ConditionVariable ] resource The resource.
        attr_reader :connection_available_condvar

        # Number of connections that the pool has which are ready to be
        # checked out. This is NOT the size of the connection pool (total
        # number of active connections created by the pool).
        def_delegators :connections, :size

        # Number of connections that the pool has which are ready to be
        # checked out.
        #
        # @since 2.7.0
        alias_method :stack_size, :size

        # Number of connections in the pool (active connections ready to
        # be checked out plus connections already checked out).
        #
        # @since 2.7.0
        attr_reader :pool_size

        # Retrieves a connection. If there are active connections in the
        # stack, the most recently used connection is returned. Otherwise
        # if the connection pool size is less than the max size, creates a
        # new connection and returns it. Otherwise raises Timeout::Error.
        #
        # @example Pop a connection.
        #   stack.pop
        #
        # @return [ Mongo::Server::Connection ] The next connection.
        # @raise [ Timeout::Error ] If the connection pool is at maximum size
        #   and remains so for longer than the wait timeout.
        #
        # @since 2.0.0
        def pop(deadline)
          check_count_invariants
          pop_connection(deadline)
        ensure
          check_count_invariants
        end

        # Updates the generation number. The connections will be disconnected and removed lazily
        # when the stack attempts to pop them.
        #
        # @since 2.8.0
        def clear
          @generation += 1
        end

        # Disconnect all connections in the stack.
        #
        # @example Disconnect all connections.
        #   stack.close!
        #
        # @return [ true ] Always true.
        #
        # @since 2.8.0
        def close!
          check_count_invariants

          @generation += 1

          mutex.synchronize do
            until connections.empty?
              close_connection(connections.shift, Monitoring::Event::Cmap::ConnectionClosed::POOL_CLOSED)
            end

            true
          end
        ensure
          check_count_invariants
        end

        # Push a connection onto the stack.
        #
        # Only connections created by this stack should be pushed
        # back into it, however the stack does not verify whether it
        # originally created the connection being pushed.
        #
        # If linting is enabled (see Mongo::Lint), attempting to push
        # connections beyond the pool's capacity will raise Mongo::Error::LintError
        # (since some of those connections must not have originated from
        # the stack into which they are being pushed). If linting is
        # not enabled, the stack can grow beyond its max size with undefined
        # results.
        #
        # @example Push a connection.
        #   stack.push(connection)
        #
        # @param [ Mongo::Server::Connection ] connection The connection.
        #
        # @since 2.0.0
        def push(connection)
          check_count_invariants
          mutex.synchronize do
            if connection.generation == @generation
              connections.unshift(connection.record_checkin!)
            else
              close_connection(connection, Monitoring::Event::Cmap::ConnectionClosed::STALE)
            end

            connection_available_condvar.broadcast
          end
          nil
        ensure
          check_count_invariants
        end

        # Get a pretty printed string inspection for the stack.
        #
        # @example Inspect the stack.
        #   stack.inspect
        #
        # @return [ String ] The stack inspection.
        #
        # @since 2.0.0
        def inspect
          "#<Mongo::Server::ConnectionPool::AvailableStack:0x#{object_id} min_size=#{min_size} max_size=#{max_size} " +
            "current_size=#{stack_size}>"
        end

        # Get the maximum size of the connection pool.
        #
        # @example Get the max size.
        #   stack.max_size
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
        #   stack.min_size
        #
        # @return [ Integer ] The minimum size of the connection pool.
        #
        # @since 2.0.0
        def min_size
          @min_size ||= options[:min_pool_size] || MIN_SIZE
        end

        # The maximum seconds a socket can remain idle since it has been
        # checked in to the pool.
        #
        # @example Get the max idle time.
        #   stack.max_idle_time
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
        #   stack.close_stale_sockets!
        #
        # @since 2.5.0
        def close_stale_sockets!
          check_count_invariants
          return unless max_idle_time

          mutex.synchronize do
            to_refresh = []
            connections.reject! do |connection|
              if last_checkin = connection.last_checkin
                if (Time.now - last_checkin) > max_idle_time
                  to_refresh << connection
                end
              end
            end

            to_refresh.each do |connection|
              close_connection(connection, Monitoring::Event::Cmap::ConnectionClosed::STALE)
            end
          end
        ensure
          check_count_invariants
        end

        def connection_removed
          @pool_size -= 1
          if @pool_size < 0
            # This should never happen
            log_warn("ConnectionPool::AvailableStack: unexpected connection removal")
            @pool_size = 0
          end
        end

        private

        def close_connection(connection, reason)
          connection_removed
          connection.disconnect!

          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionClosed.new(
              reason,
              @address,
              connection.id,
            )
          )
        end

        def close_if_stale!(connection)
          if connection.generation != @generation
            close_connection(connection, Monitoring::Event::Cmap::ConnectionClosed::STALE)
            true
          end
        end

        def close_if_idle!(connection)
          if connection && connection.last_checkin && max_idle_time
            if Time.now - connection.last_checkin > max_idle_time
              close_connection(connection, Monitoring::Event::Cmap::ConnectionClosed::IDLE)
              true
            end
          end
        end

        def pop_connection(deadline)
          mutex.synchronize do
            get_connection(deadline)
          end
        end

        def get_connection(deadline)
          loop do
            until connections.empty?
              connection = connections.shift
              return connection unless close_if_stale!(connection) || close_if_idle!(connection)
            end

            connection = create_connection
            return connection if connection

            wait = deadline - Time.now
            connection_available_condvar.wait(mutex, wait)

            raise Error::ConnectionCheckoutTimeout.new(@address) if deadline <= Time.now
          end
        end

        def create_connection
          if pool_size < max_size
            @pool_size += 1
            @block.call(@generation)
          end
        end

        # We only create new connections when we're below the minPoolSize on creation, when we're
        # disconnecting and starting a new generation, and when we're checking out connections (per
        # the CMAP spec), so `check_min` should be false in all other cases.
        def check_count_invariants(check_min = false)
          if Mongo::Lint.enabled?
            if pool_size < min_size && check_min
              raise Error::LintError, 'connection pool stack: underflow'
            end
            if pool_size > max_size
              raise Error::LintError, 'connection pool stack: overflow'
            end
          end
        end
      end
    end
  end
end
