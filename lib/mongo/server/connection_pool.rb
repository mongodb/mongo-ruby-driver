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

    # Represents a connection pool for server connections.
    #
    # @since 2.0.0, largely rewritten in 2.9.0
    class ConnectionPool
      include Loggable
      include Monitoring::Publishable
      extend Forwardable

      # The default max size for the connection pool.
      #
      # @since 2.9.0
      DEFAULT_MAX_SIZE = 5.freeze

      # The default min size for the connection pool.
      #
      # @since 2.9.0
      DEFAULT_MIN_SIZE = 0.freeze

      # The default timeout, in seconds, to wait for a connection.
      #
      # @since 2.9.0
      DEFAULT_WAIT_TIMEOUT = 1.freeze

      # Create the new connection pool.
      #
      # @param [ Server ] server The server which this connection pool is for.
      # @param [ Hash ] options The connection pool options.
      #
      # @option options [ Integer ] :max_size The maximum pool size.
      # @option options [ Integer ] :max_pool_size Deprecated.
      #   The maximum pool size. If max_size is also given, max_size and
      #   max_pool_size must be identical.
      # @option options [ Integer ] :min_size The minimum pool size.
      # @option options [ Integer ] :min_pool_size Deprecated.
      #   The minimum pool size. If min_size is also given, min_size and
      #   min_pool_size must be identical.
      # @option options [ Float ] :wait_timeout The time to wait, in
      #   seconds, for a free connection.
      # @option options [ Float ] :wait_queue_timeout Deprecated.
      #   Alias for :wait_timeout. If both wait_timeout and wait_queue_timeout
      #   are given, their values must be identical.
      # @option options [ Float ] :max_idle_time The time, in seconds,
      #   after which idle connections should be closed by the pool.
      #
      # @since 2.0.0, API changed in 2.9.0
      def initialize(server, options = {})
        unless server.is_a?(Server)
          raise ArgumentError, 'First argument must be a Server instance'
        end
        options = options.dup
        if options[:min_size] && options[:min_pool_size] && options[:min_size] != options[:min_pool_size]
          raise ArgumentError, "Min size #{options[:min_size]} is not identical to min pool size #{options[:min_pool_size]}"
        end
        if options[:max_size] && options[:max_pool_size] && options[:max_size] != options[:max_pool_size]
          raise ArgumentError, "Max size #{options[:max_size]} is not identical to max pool size #{options[:max_pool_size]}"
        end
        if options[:wait_timeout] && options[:wait_queue_timeout] && options[:wait_timeout] != options[:wait_queue_timeout]
          raise ArgumentError, "Wait timeout #{options[:wait_timeout]} is not identical to wait queue timeout #{options[:wait_queue_timeout]}"
        end
        options[:min_size] ||= options[:min_pool_size]
        options.delete(:min_pool_size)
        options[:max_size] ||= options[:max_pool_size]
        options.delete(:max_pool_size)
        if options[:min_size] && options[:max_size] &&
          options[:min_size] > options[:max_size]
        then
          raise ArgumentError, "Cannot have min size #{options[:min_size]} exceed max size #{options[:max_size]}"
        end
        if options[:wait_queue_timeout]
          options[:wait_timeout] ||= options[:wait_queue_timeout]
        end
        options.delete(:wait_queue_timeout)

        @server = server
        @options = options.freeze

        @generation = 1
        @closed = false

        # A connection owned by this pool should be either in the
        # available connections array (which is used as a stack)
        # or in the checked out connections set.
        @available_connections = available_connections = []
        @checked_out_connections = Set.new

        # Mutex used for synchronizing access to @available_connections and
        # @checked_out_connections. The pool object is thread-safe, thus
        # all methods that retrieve or modify instance variables generally
        # must do so under this lock.
        @lock = Mutex.new

        # Condition variable broadcast when a connection is added to
        # @available_connections, to wake up any threads waiting for an
        # available connection when pool is at max size
        @available_semaphore = Semaphore.new

        finalizer = proc do
          available_connections.each do |connection|
            connection.disconnect!(reason: :pool_closed)
          end
          available_connections.clear
          # Finalizer does not close checked out connections.
          # Those would have to be garbage collected on their own
          # and that should close them.
        end
        ObjectSpace.define_finalizer(self, finalizer)

        publish_cmap_event(
          Monitoring::Event::Cmap::PoolCreated.new(@server.address, options)
        )
      end

      # @return [ Hash ] options The pool options.
      attr_reader :options

      # Get the maximum size of the connection pool.
      #
      # @return [ Integer ] The maximum size of the connection pool.
      #
      # @since 2.9.0
      def max_size
        @max_size ||= options[:max_size] || [DEFAULT_MAX_SIZE, min_size].max
      end

      # Get the minimum size of the connection pool.
      #
      # @return [ Integer ] The minimum size of the connection pool.
      #
      # @since 2.9.0
      def min_size
        @min_size ||= options[:min_size] || DEFAULT_MIN_SIZE
      end

      # The time to wait, in seconds, for a connection to become available.
      #
      # @return [ Float ] The queue wait timeout.
      #
      # @since 2.9.0
      def wait_timeout
        @wait_timeout ||= options[:wait_timeout] || DEFAULT_WAIT_TIMEOUT
      end

      # The maximum seconds a socket can remain idle since it has been
      # checked in to the pool, if set.
      #
      # @return [ Float | nil ] The max socket idle time in seconds.
      #
      # @since 2.9.0
      def max_idle_time
        @max_idle_time ||= options[:max_idle_time]
      end

      # @return [ Integer ] generation Generation of connections currently
      #   being used by the queue.
      #
      # @since 2.9.0
      # @api private
      attr_reader :generation

      # Size of the connection pool.
      #
      # Includes available and checked out connections.
      #
      # @return [ Integer ] Size of the connection pool.
      #
      # @since 2.9.0
      def size
        raise_if_closed!

        @lock.synchronize do
          unsynchronized_size
        end
      end

      # Returns the size of the connection pool without acquiring the lock.
      # This method should only be used by other pool methods when they are
      # already holding the lock as Ruby does not allow a thread holding a
      # lock to acquire this lock again.
      def unsynchronized_size
        @available_connections.length + @checked_out_connections.size
      end
      private :unsynchronized_size

      # Number of available connections in the pool.
      #
      # @return [ Integer ] Number of available connections.
      #
      # @since 2.9.0
      def available_count
        raise_if_closed!

        @lock.synchronize do
          @available_connections.length
        end
      end

      # Whether the pool has been closed.
      #
      # @return [ true | false ] Whether the pool is closed.
      #
      # @since 2.9.0
      def closed?
        !!@closed
      end

      # @since 2.9.0
      def_delegators :@server, :monitoring

      # Checks a connection out of the pool.
      #
      # If there are active connections in the pool, the most recently used
      # connection is returned. Otherwise if the connection pool size is less
      # than the max size, creates a new connection and returns it. Otherwise
      # waits up to the wait timeout and raises Timeout::Error if there are
      # still no active connections and the pool is at max size.
      #
      # The returned connection counts toward the pool's max size. When the
      # caller is finished using the connection, the connection should be
      # checked back in via the check_in method.
      #
      # @return [ Mongo::Server::Connection ] The checked out connection.
      # @raise [ Timeout::Error ] If the connection pool is at maximum size
      #   and remains so for longer than the wait timeout.
      #
      # @since 2.9.0
      def check_out
        raise_if_closed!

        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckOutStarted.new(@server.address)
        )

        deadline = Time.now + wait_timeout
        connection = nil
        # It seems that synchronize sets up its own loop, thus a simple break
        # is insufficient to break the outer loop
        catch(:done) do
          loop do
            # Lock must be taken on each iteration, rather for the method
            # overall, otherwise other threads will not be able to check in
            # a connection while this thread is waiting for one.
            @lock.synchronize do
              until @available_connections.empty?
                connection = @available_connections.pop

                if connection.generation != generation
                  # Stale connections should be disconnected in the clear
                  # method, but if any don't, check again here
                  connection.disconnect!(reason: :stale)
                  next
                end

                if max_idle_time && connection.last_checkin &&
                  Time.now - connection.last_checkin > max_idle_time
                then
                  connection.disconnect!(reason: :idle)
                  next
                end

                throw(:done)
              end

              # Ruby does not allow a thread to lock a mutex which it already
              # holds.
              if unsynchronized_size < max_size
                # This does not currently connect the socket and handshake,
                # but if it did, it would be performing i/o under our lock,
                # which is bad. Fix in the future.
                connection = create_connection
                throw(:done)
              end
            end

            wait = deadline - Time.now
            if wait <= 0
              publish_cmap_event(
                Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
                  @server.address,
                  Monitoring::Event::Cmap::ConnectionCheckOutFailed::TIMEOUT,
                ),
              )
              raise Error::ConnectionCheckOutTimeout.new(@server.address, wait_timeout)
            end
            @available_semaphore.wait(wait)
          end
        end

        @checked_out_connections << connection
        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckedOut.new(@server.address, connection.id),
        )
        connection
      end

      # Check a connection back into the pool.
      #
      # The connection must have been previously created by this pool.
      #
      # @param [ Mongo::Server::Connection ] connection The connection.
      #
      # @since 2.9.0
      def check_in(connection)
        @lock.synchronize do
          unless @checked_out_connections.include?(connection)
            raise ArgumentError, "Trying to check in a connection which is not currently checked out by this pool: #{connection}"
          end

          @checked_out_connections.delete(connection)

          # Note: if an event handler raises, resource will not be signaled.
          # This means threads waiting for a connection to free up when
          # the pool is at max size may time out.
          # Threads that begin waiting after this method completes (with
          # the exception) should be fine.
          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionCheckedIn.new(@server.address, connection.id)
          )

          if closed?
            connection.disconnect!(reason: :pool_closed)
            return
          end

          if connection.closed?
            # Connection was closed - for example, because it experienced
            # a network error. Nothing else needs to be done here.
          elsif connection.generation != @generation
            connection.disconnect!(reason: :stale)
          else
            connection.record_checkin!
            @available_connections << connection

            # Wake up only one thread waiting for an available connection,
            # since only one connection was checked in.
            @available_semaphore.signal
          end
        end
      end

      # Closes all idle connections in the pool and schedules currently checked
      # out connections to be closed when they are checked back into the pool.
      # The pool remains operational and can create new connections when
      # requested.
      #
      # @option options [ true | false ] :lazy If true, do not close any of
      #   the idle connections and instead let them be closed during a
      #   subsequent check out operation.
      #
      # @return [ true ] true.
      #
      # @since 2.1.0
      def clear(options = nil)
        raise_if_closed!

        @lock.synchronize do
          @generation += 1

          publish_cmap_event(
            Monitoring::Event::Cmap::PoolCleared.new(@server.address)
          )

          unless options && options[:lazy]
            until @available_connections.empty?
              connection = @available_connections.pop
              connection.disconnect!(reason: :stale)
            end
          end
        end

        true
      end

      # @since 2.1.0
      # @deprecated
      alias :disconnect! :clear

      # Marks the pool closed, closes all idle connections in the pool and
      # schedules currently checked out connections to be closed when they are
      # checked back into the pool. If force option is true, checked out
      # connections are also closed. Attempts to use the pool after it is closed
      # will raise Error::PoolClosedError.
      #
      # @option options [ true | false ] :force Also close all checked out
      #   connections.
      #
      # @return [ true ] true.
      #
      # @since 2.9.0
      def close(options = nil)
        return if closed?

        @lock.synchronize do
          until @available_connections.empty?
            connection = @available_connections.pop
            connection.disconnect!(reason: :pool_closed)
          end

          if options && options[:force]
            until @checked_out_connections.empty?
              connection = @checked_out_connections.take(1).first
              connection.disconnect!(reason: :pool_closed)
              @checked_out_connections.delete(connection)
            end
          end
        end

        @closed = true

        publish_cmap_event(
          Monitoring::Event::Cmap::PoolClosed.new(@server.address)
        )

        true
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
        if closed?
          "#<Mongo::Server::ConnectionPool:0x#{object_id} min_size=#{min_size} max_size=#{max_size} " +
            "wait_timeout=#{wait_timeout} closed>"
        else
          "#<Mongo::Server::ConnectionPool:0x#{object_id} min_size=#{min_size} max_size=#{max_size} " +
            "wait_timeout=#{wait_timeout} current_size=#{size} available=#{available_count}>"
        end
      end

      # Yield the block to a connection, while handling check in/check out logic.
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

        connection = check_out
        yield(connection)
      ensure
        if connection
          check_in(connection)
        end
      end

      # Close sockets that have been open for longer than the max idle time,
      #   if the option is set.
      #
      # @since 2.5.0
      def close_idle_sockets
        return if closed?
        return unless max_idle_time

        @lock.synchronize do
          i = 0
          while i < @available_connections.length
            connection = @available_connections[i]
            if last_checkin = connection.last_checkin
              if (Time.now - last_checkin) > max_idle_time
                connection.disconnect!(reason: :idle)
                @available_connections.delete_at(i)
                next
              end
            end
            i += 1
          end
        end
      end

      # Creates up to the min size connections.
      #
      # Used by the spec test runner.
      #
      # @api private
      def populate
        while size < min_size
          @available_connections << create_connection
        end
      end

      private

      def create_connection
        connection = Connection.new(@server, options.merge(generation: generation))
        # CMAP spec requires connections to be returned from the pool
        # fully established.
        #connection.connect!
        connection
      end

      # Asserts that the pool has not been closed.
      #
      # @raise [ Error::PoolClosedError ] If the pool has been closed.
      #
      # @since 2.9.0
      def raise_if_closed!
        if closed?
          raise Error::PoolClosedError.new(@server.address)
        end
      end
    end
  end
end
