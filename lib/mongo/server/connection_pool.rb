# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
      DEFAULT_MAX_SIZE = 20

      # The default min size for the connection pool.
      #
      # @since 2.9.0
      DEFAULT_MIN_SIZE = 0

      # The default maximum number of connections that can be connecting at
      # any given time.
      DEFAULT_MAX_CONNECTING = 2

      # The default timeout, in seconds, to wait for a connection.
      #
      # This timeout applies while in flow threads are waiting for background
      # threads to establish connections (and hence they must connect, handshake
      # and auth in the allotted time).
      #
      # It is currently set to 10 seconds. The default connect timeout is
      # 10 seconds by itself, but setting large timeouts can get applications
      # in trouble if their requests get timed out by the reverse proxy,
      # thus anything over 15 seconds is potentially dangerous.
      #
      # @since 2.9.0
      DEFAULT_WAIT_TIMEOUT = 10.freeze

      # Condition variable broadcast when the size of the pool changes
      # to wake up the populator
      attr_reader :populate_semaphore

      # Create the new connection pool.
      #
      # @param [ Server ] server The server which this connection pool is for.
      # @param [ Hash ] options The connection pool options.
      #
      # @option options [ Integer ] :max_size The maximum pool size. Setting
      #   this option to zero creates an unlimited connection pool.
      # @option options [ Integer ] :max_connecting The maximum number of
      #  connections that can be connecting simultaneously. The default is 2.
      #  This option should be increased if there are many threads that share
      #  same connection pool and the application is experiencing timeouts
      #  while waiting for connections to be established.
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
      # @option options [ true, false ] :populator_io For internal driver
      #   use only. Set to false to prevent the populator threads from being
      #   created and started in the server's connection pool. It is intended
      #   for use in tests that also turn off monitoring_io, unless the populator
      #   is explicitly needed. If monitoring_io is off, but the populator_io
      #   is on, the populator needs to be manually closed at the end of the
      #   test, since a cluster without monitoring is considered not connected,
      #   and thus will not clean up the connection pool populator threads on
      #   close.
      # Note: Additionally, options for connections created by this pool should
      #   be included in the options passed here, and they will be forwarded to
      #   any connections created by the pool.
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
          (options[:max_size] != 0 && options[:min_size] > options[:max_size])
        then
          raise ArgumentError, "Cannot have min size #{options[:min_size]} exceed max size #{options[:max_size]}"
        end
        if options[:wait_queue_timeout]
          options[:wait_timeout] ||= options[:wait_queue_timeout]
        end
        options.delete(:wait_queue_timeout)

        @server = server
        @options = options.freeze

        @generation_manager = GenerationManager.new(server: server)
        @ready = false
        @closed = false

        # A connection owned by this pool should be either in the
        # available connections array (which is used as a stack)
        # or in the checked out connections set.
        @available_connections = available_connections = []
        @checked_out_connections = Set.new
        @pending_connections = Set.new
        @interrupt_connections = []

        # Mutex used for synchronizing access to @available_connections and
        # @checked_out_connections. The pool object is thread-safe, thus
        # all methods that retrieve or modify instance variables generally
        # must do so under this lock.
        @lock = Mutex.new

        # Background thread reponsible for maintaining the size of
        # the pool to at least min_size
        @populator = Populator.new(self, options)
        @populate_semaphore = Semaphore.new

        # Condition variable to enforce the first check in check_out: max_pool_size.
        # This condition variable should be signaled when the number of
        # unavailable connections decreases (pending + pending_connections +
        # checked_out_connections).
        @size_cv = Mongo::ConditionVariable.new(@lock)
        # This represents the number of threads that have made it past the size_cv
        # gate but have not acquired a connection to add to the pending_connections
        # set.
        @connection_requests = 0

        # Condition variable to enforce the second check in check_out: max_connecting.
        # Thei condition variable should be signaled when the number of pending
        # connections decreases.
        @max_connecting_cv = Mongo::ConditionVariable.new(@lock)
        @max_connecting = options.fetch(:max_connecting, DEFAULT_MAX_CONNECTING)

        ObjectSpace.define_finalizer(self, self.class.finalize(@available_connections, @pending_connections, @populator))

        publish_cmap_event(
          Monitoring::Event::Cmap::PoolCreated.new(@server.address, options, self)
        )
      end

      # @return [ Hash ] options The pool options.
      attr_reader :options

      # @api private
      attr_reader :server

      # @api private
      def_delegators :server, :address

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

      # @api private
      attr_reader :generation_manager

      # @return [ Integer ] generation Generation of connections currently
      #   being used by the queue.
      #
      # @api private
      def_delegators :generation_manager, :generation, :generation_unlocked

      # A connection pool is paused if it is not closed and it is not ready.
      #
      # @return [ true | false ] whether the connection pool is paused.
      #
      # @raise [ Error::PoolClosedError ] If the pool has been closed.
      def paused?
        raise_if_closed!

        @lock.synchronize do
          !@ready
        end
      end

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
        @available_connections.length + @checked_out_connections.length + @pending_connections.length
      end
      private :unsynchronized_size

      # @return [ Integer ] The number of unavailable connections in the pool.
      #   Used to calculate whether we have hit max_pool_size.
      #
      # @api private
      def unavailable_connections
        @checked_out_connections.length + @pending_connections.length + @connection_requests
      end

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

      # Whether the pool is ready.
      #
      # @return [ true | false ] Whether the pool is ready.
      def ready?
        @lock.synchronize do
          @ready
        end
      end

      # @note This method is experimental and subject to change.
      #
      # @api experimental
      # @since 2.11.0
      def summary
        @lock.synchronize do
          state = if closed?
            'closed'
          elsif !@ready
            'paused'
          else
            'ready'
          end
          "#<ConnectionPool size=#{unsynchronized_size} (#{min_size}-#{max_size}) " +
            "used=#{@checked_out_connections.length} avail=#{@available_connections.length} pending=#{@pending_connections.length} #{state}>"
        end
      end

      # @since 2.9.0
      def_delegators :@server, :monitoring

      # @api private
      attr_reader :populator

      # @api private
      attr_reader :max_connecting

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
      # @raise [ Error::PoolClosedError ] If the pool has been closed.
      # @raise [ Timeout::Error ] If the connection pool is at maximum size
      #   and remains so for longer than the wait timeout.
      #
      # @since 2.9.0
      def check_out(connection_global_id: nil)
        check_invariants

        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckOutStarted.new(@server.address)
        )

        raise_if_pool_closed!
        raise_if_pool_paused_locked!

        connection = retrieve_and_connect_connection(connection_global_id)

        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckedOut.new(@server.address, connection.id, self),
        )

        if Lint.enabled?
          unless connection.connected?
            raise Error::LintError, "Connection pool for #{address} checked out a disconnected connection #{connection.generation}:#{connection.id}"
          end
        end

        connection
      ensure
        check_invariants
      end

      # Check a connection back into the pool.
      #
      # The connection must have been previously created by this pool.
      #
      # @param [ Mongo::Server::Connection ] connection The connection.
      #
      # @since 2.9.0
      def check_in(connection)
        check_invariants

        @lock.synchronize do
          do_check_in(connection)
        end
      ensure
        check_invariants
      end

      # Executes the check in after having already acquired the lock.
      #
      # @param [ Mongo::Server::Connection ] connection The connection.
      def do_check_in(connection)
        # When a connection is interrupted it is checked back into the pool
        # and closed. The operation that was using the connection before it was
        # interrupted will attempt to check it back into the pool, and we
        # should ignore it since its already been closed and removed from the pool.
        return if connection.closed? && connection.interrupted?

        unless connection.connection_pool == self
          raise ArgumentError, "Trying to check in a connection which was not checked out by this pool: #{connection} checked out from pool #{connection.connection_pool} (for #{self})"
        end

        unless @checked_out_connections.include?(connection)
          raise ArgumentError, "Trying to check in a connection which is not currently checked out by this pool: #{connection} (for #{self})"
        end

        # Note: if an event handler raises, resource will not be signaled.
        # This means threads waiting for a connection to free up when
        # the pool is at max size may time out.
        # Threads that begin waiting after this method completes (with
        # the exception) should be fine.

        @checked_out_connections.delete(connection)
        @size_cv.signal

        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckedIn.new(@server.address, connection.id, self)
        )

        if connection.interrupted?
          connection.disconnect!(reason: :stale)
          return
        end

        if connection.error?
          connection.disconnect!(reason: :error)
          return
        end

        if closed?
          connection.disconnect!(reason: :pool_closed)
          return
        end

        if connection.closed?
          # Connection was closed - for example, because it experienced
          # a network error. Nothing else needs to be done here.
          @populate_semaphore.signal
        elsif connection.generation != generation(service_id: connection.service_id) && !connection.pinned?
          # If connection is marked as pinned, it is used by a transaction
          # or a series of cursor operations in a load balanced setup.
          # In this case connection should not be disconnected until
          # unpinned.
          connection.disconnect!(reason: :stale)
          @populate_semaphore.signal
        else
          connection.record_checkin!
          @available_connections << connection

          @max_connecting_cv.signal
        end
      end

      # Mark the connection pool as paused.
      def pause
        raise_if_closed!

        check_invariants

        @lock.synchronize do
          do_pause
        end
      ensure
        check_invariants
      end

      # Mark the connection pool as paused without acquiring the lock.
      #
      # @api private
      def do_pause
        if Lint.enabled? && !@server.unknown?
          raise Error::LintError, "Attempting to pause pool for server #{@server.summary} which is known"
        end

        return if !@ready

        @ready = false
      end

      # Closes all idle connections in the pool and schedules currently checked
      # out connections to be closed when they are checked back into the pool.
      # The pool is paused, it will not create new connections in background
      # and it will fail checkout requests until marked ready.
      #
      # @option options [ true | false ] :lazy If true, do not close any of
      #   the idle connections and instead let them be closed during a
      #   subsequent check out operation. Defaults to false.
      # @option options [ true | false ] :interrupt_in_use_connections If true,
      #   close all checked out connections immediately. If it is false, do not
      #   close any of the checked out connections. Defaults to true.
      # @option options [ Object ] :service_id Clear connections with
      #   the specified service id only.
      #
      # @return [ true ] true.
      #
      # @since 2.1.0
      def clear(options = nil)
        raise_if_closed!

        if Lint.enabled? && !@server.unknown?
          raise Error::LintError, "Attempting to clear pool for server #{@server.summary} which is known"
        end

        do_clear(options)
      end

      # Disconnects the pool.
      #
      # Does everything that +clear+ does, except if the pool is closed
      # this method does nothing but +clear+ would raise PoolClosedError.
      #
      # @since 2.1.0
      # @api private
      def disconnect!(options = nil)
        do_clear(options)
      rescue Error::PoolClosedError
        # The "disconnected" state is between closed and paused.
        # When we are trying to disconnect the pool, permit the pool to be
        # already closed.
      end

      def do_clear(options = nil)
        check_invariants

        service_id = options && options[:service_id]

        @lock.synchronize do
          # Generation must be bumped before emitting pool cleared event.
          @generation_manager.bump(service_id: service_id)

          unless options && options[:lazy]
            close_available_connections(service_id)
          end

          if options && options[:interrupt_in_use_connections]
            schedule_for_interruption(@checked_out_connections, service_id)
            schedule_for_interruption(@pending_connections, service_id)
          end

          if @ready
            publish_cmap_event(
              Monitoring::Event::Cmap::PoolCleared.new(
                @server.address,
                service_id: service_id,
                interrupt_in_use_connections: options&.[](:interrupt_in_use_connections)
              )
            )
            # Only pause the connection pool if the server was marked unknown,
            # otherwise, allow the retry to be attempted with a ready pool.
            do_pause if !@server.load_balancer? && @server.unknown?
          end

          # Broadcast here to cause all of the threads waiting on the max
          # connecting to break out of the wait loop and error.
          @max_connecting_cv.broadcast
          # Broadcast here to cause all of the threads waiting on the pool size
          # to break out of the wait loop and error.
          @size_cv.broadcast
        end

        # "Schedule the background thread" after clearing. This is responsible
        # for cleaning up stale threads, and interrupting in use connections.
        @populate_semaphore.signal
        true
      ensure
        check_invariants
      end

      # Instructs the pool to create and return connections.
      def ready
        raise_if_closed!

        # TODO: Add this back in RUBY-3174.
        # if Lint.enabled?
        #   unless @server.connected?
        #     raise Error::LintError, "Attempting to ready a pool for server #{@server.summary} which is disconnected"
        #   end
        # end

        @lock.synchronize do
          return if @ready

          @ready = true
        end

        # Note that the CMAP spec demands serialization of CMAP events for a
        # pool. In order to implement this, event publication must be done into
        # a queue which is synchronized, instead of subscribers being invoked
        # from the trigger method like this one here inline. On MRI, assuming
        # the threads yield to others when they stop having work to do, it is
        # likely that the events would in practice always be published in the
        # required order. JRuby, being truly concurrent with OS threads,
        # would not offers such a guarantee.
        publish_cmap_event(
          Monitoring::Event::Cmap::PoolReady.new(@server.address, options, self)
        )

        if options.fetch(:populator_io, true)
          if @populator.running?
            @populate_semaphore.signal
          else
            @populator.run!
          end
        end
      end

      # Marks the pool closed, closes all idle connections in the pool and
      # schedules currently checked out connections to be closed when they are
      # checked back into the pool. If force option is true, checked out
      # connections are also closed. Attempts to use the pool after it is closed
      # will raise Error::PoolClosedError.
      #
      # @option options [ true | false ] :force Also close all checked out
      #   connections.
      # @option options [ true | false ] :stay_ready For internal driver use
      #    only. Whether or not to mark the pool as closed.
      #
      # @return [ true ] Always true.
      #
      # @since 2.9.0
      def close(options = nil)
        return if closed?

        options ||= {}

        stop_populator

        @lock.synchronize do
          until @available_connections.empty?
            connection = @available_connections.pop
            connection.disconnect!(reason: :pool_closed)
          end

          if options[:force]
            until @checked_out_connections.empty?
              connection = @checked_out_connections.take(1).first
              connection.disconnect!(reason: :pool_closed)
              @checked_out_connections.delete(connection)
            end
          end

          unless options && options[:stay_ready]
            # mark pool as closed before releasing lock so
            # no connections can be created, checked in, or checked out
            @closed = true
            @ready = false
          end

          @max_connecting_cv.broadcast
          @size_cv.broadcast
        end

        publish_cmap_event(
          Monitoring::Event::Cmap::PoolClosed.new(@server.address, self)
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
        elsif !ready?
          "#<Mongo::Server::ConnectionPool:0x#{object_id} min_size=#{min_size} max_size=#{max_size} " +
            "wait_timeout=#{wait_timeout} paused>"
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
      def with_connection(connection_global_id: nil)
        raise_if_closed!

        connection = check_out(connection_global_id: connection_global_id)
        yield(connection)
      rescue Error::SocketError, Error::SocketTimeoutError, Error::ConnectionPerished => e
        maybe_raise_pool_cleared!(connection, e)
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
                @populate_semaphore.signal
                next
              end
            end
            i += 1
          end
        end
      end

      # Stop the background populator thread and clean up any connections created
      # which have not been connected yet.
      #
      # Used when closing the pool or when terminating the bg thread for testing
      # purposes. In the latter case, this method must be called before the pool
      # is used, to ensure no connections in pending_connections were created in-flow
      # by the check_out method.
      #
      # @api private
      def stop_populator
        @populator.stop!

        @lock.synchronize do
          # If stop_populator is called while populate is running, there may be
          # connections waiting to be connected, connections which have not yet
          # been moved to available_connections, or connections moved to available_connections
          # but not deleted from pending_connections. These should be cleaned up.
          clear_pending_connections
        end
      end

      # This method does three things:
      # 1. Creates and adds a connection to the pool, if the pool's size is
      #    below min_size. Retries once if a socket-related error is
      #    encountered during this process and raises if a second error or a
      #    non socket-related error occurs.
      # 2. Removes stale connections from the connection pool.
      # 3. Interrupts connections marked for interruption.
      #
      # Used by the pool populator background thread.
      #
      # @return [ true | false ] Whether this method should be called again
      #   to create more connections.
      # @raise [ Error::AuthError, Error ] The second socket-related error raised if a retry
      # occured, or the non socket-related error
      #
      # @api private
      def populate
        return false if closed?

        begin
          return create_and_add_connection
        rescue Error::SocketError, Error::SocketTimeoutError => e
          # an error was encountered while connecting the connection,
          # ignore this first error and try again.
          log_warn("Populator failed to connect a connection for #{address}: #{e.class}: #{e}. It will retry.")
        end

        return create_and_add_connection
      end

      # Finalize the connection pool for garbage collection.
      #
      # @param [ List<Mongo::Connection> ] available_connections The available connections.
      # @param [ List<Mongo::Connection> ] pending_connections The pending connections.
      # @param [ Populator ] populator The populator.
      #
      # @return [ Proc ] The Finalizer.
      def self.finalize(available_connections, pending_connections, populator)
        proc do
          available_connections.each do |connection|
            connection.disconnect!(reason: :pool_closed)
          end
          available_connections.clear

          pending_connections.each do |connection|
            connection.disconnect!(reason: :pool_closed)
          end
          pending_connections.clear

          # Finalizer does not close checked out connections.
          # Those would have to be garbage collected on their own
          # and that should close them.
        end
      end

      private

      # Returns the next available connection, optionally with given
      # global id. If no suitable connections are available,
      # returns nil.
      def next_available_connection(connection_global_id)
        raise_unless_locked!

        if @server.load_balancer? && connection_global_id
          conn = @available_connections.detect do |conn|
            conn.global_id == connection_global_id
          end
          if conn
            @available_connections.delete(conn)
          end
          conn
        else
          @available_connections.pop
        end
      end

      def create_connection
        r, _ = @generation_manager.pipe_fds(service_id: server.description.service_id)
        opts = options.merge(
          connection_pool: self,
          pipe: r
          # Do not pass app metadata - this will be retrieved by the connection
          # based on the auth needs.
        )
        unless @server.load_balancer?
          opts[:generation] = generation
        end
        Connection.new(@server, opts)
      end

      # Create a connection, connect it, and add it to the pool. Also
      # check for stale and interruptable connections and deal with them.
      #
      # @return [ true | false ] True if a connection was created and
      #    added to the pool, false otherwise
      # @raise [ Mongo::Error ] An error encountered during connection connect
      def create_and_add_connection
        connection = nil

        @lock.synchronize do
          if !closed? && @ready &&
            (unsynchronized_size + @connection_requests) < min_size &&
            @pending_connections.length < @max_connecting
          then
            connection = create_connection
            @pending_connections << connection
          else
            return true if remove_interrupted_connections
            return true if remove_stale_connection
            return false
          end
        end

        begin
          connect_connection(connection)
        rescue Exception
          @lock.synchronize do
            @pending_connections.delete(connection)
            @max_connecting_cv.signal
            @size_cv.signal
          end
          raise
        end

        @lock.synchronize do
          @available_connections << connection
          @pending_connections.delete(connection)
          @max_connecting_cv.signal
          @size_cv.signal
        end

        true
      end

      # Removes and disconnects all stale available connections.
      def remove_stale_connection
        if conn = @available_connections.detect(&method(:connection_stale_unlocked?))
          conn.disconnect!(reason: :stale)
          @available_connections.delete(conn)
          return true
        end
      end

      # Interrupt connections scheduled for interruption.
      def remove_interrupted_connections
        return false if @interrupt_connections.empty?

        gens = Set.new
        while conn = @interrupt_connections.pop
          if @checked_out_connections.include?(conn)
            # If the connection has been checked out, mark it as interrupted and it will
            # be disconnected on check in.
            conn.interrupted!
            do_check_in(conn)
          elsif @pending_connections.include?(conn)
            # If the connection is pending, disconnect with the interrupted flag.
            conn.disconnect!(reason: :stale, interrupted: true)
            @pending_connections.delete(conn)
          end
          gens << [ conn.generation, conn.service_id ]
        end

        # Close the write side of the pipe. Pending connections might be
        # hanging on the Kernel#select call, so in order to interrupt that,
        # we also listen for the read side of the pipe in Kernel#select and
        # close the write side of the pipe here, which will cause select to
        # wake up and raise an IOError now that the socket is closed.
        # The read side of the pipe will be scheduled for closing on the next
        # generation bump.
        gens.each do |gen, service_id|
          @generation_manager.remove_pipe_fds(gen, service_id: service_id)
        end

        true
      end

      # Checks whether a connection is stale.
      #
      # @param [ Mongo::Server::Connection ] connection The connection to check.
      #
      # @return [ true | false ] Whether the connection is stale.
      def connection_stale_unlocked?(connection)
        connection.generation != generation_unlocked(service_id: connection.service_id) &&
        !connection.pinned?
      end

      # Asserts that the pool has not been closed.
      #
      # @raise [ Error::PoolClosedError ] If the pool has been closed.
      #
      # @since 2.9.0
      def raise_if_closed!
        if closed?
          raise Error::PoolClosedError.new(@server.address, self)
        end
      end

      # If the connection was interrupted, raise a pool cleared error. If it
      # wasn't interrupted raise the original error.
      #
      # @param [ Connection ] The connection.
      # @param [ Mongo::Error ] The original error.
      #
      # @raise [ Mongo::Error | Mongo::Error::PoolClearedError ] A PoolClearedError
      #   if the connection was interrupted, the original error if not.
      def maybe_raise_pool_cleared!(connection, e)
        if connection&.interrupted?
          err = Error::PoolClearedError.new(connection.server.address, connection.server.pool_internal).tap do |err|
            e.labels.each { |l| err.add_label(l) }
          end
          raise err
        else
          raise e
        end
      end

      # Attempts to connect (handshake and auth) the connection. If an error is
      # encountered, closes the connection and raises the error.
      def connect_connection(connection)
        begin
          connection.connect!
        rescue Exception
          connection.disconnect!(reason: :error)
          raise
        end
      rescue Error::SocketError, Error::SocketTimeoutError => exc
        @server.unknown!(
          generation: exc.generation,
          service_id: exc.service_id,
          stop_push_monitor: true,
        )
        raise
      end

      def check_invariants
        return unless Lint.enabled?

        # Server summary calls pool summary which requires pool lock -> deadlock.
        # Obtain the server summary ahead of time.
        server_summary = @server.summary

        @lock.synchronize do
          @available_connections.each do |connection|
            if connection.closed?
              raise Error::LintError, "Available connection is closed: #{connection} for #{server_summary}"
            end
          end

          @pending_connections.each do |connection|
            if connection.closed?
              raise Error::LintError, "Pending connection is closed: #{connection} for #{server_summary}"
            end
          end
        end
      end

      # Close the available connections.
      #
      # @param [ Array<Connection> ] connections A list of connections.
      # @param [ Object ] service_id The service id.
      def close_available_connections(service_id)
        if @server.load_balancer? && service_id
          loop do
            conn = @available_connections.detect do |conn|
              conn.service_id == service_id &&
              conn.generation < @generation_manager.generation(service_id: service_id)
            end
            if conn
              @available_connections.delete(conn)
              conn.disconnect!(reason: :stale, interrupted: true)
              @populate_semaphore.signal
            else
              break
            end
          end
        else
          @available_connections.delete_if do |conn|
            if conn.generation < @generation_manager.generation(service_id: service_id)
              conn.disconnect!(reason: :stale, interrupted: true)
              @populate_semaphore.signal
              true
            end
          end
        end
      end

      # Schedule connections of previous generations for interruption.
      #
      # @param [ Array<Connection> ] connections A list of connections.
      # @param [ Object ] service_id The service id.
      def schedule_for_interruption(connections, service_id)
        @interrupt_connections += connections.select do |conn|
          (!server.load_balancer? || conn.service_id == service_id) &&
          conn.generation < @generation_manager.generation(service_id: service_id)
        end
      end

      # Clear and disconnect the pending connections.
      def clear_pending_connections
        until @pending_connections.empty?
          connection = @pending_connections.take(1).first
          connection.disconnect!
          @pending_connections.delete(connection)
        end
      end

      # The lock should be acquired when calling this method.
      def raise_check_out_timeout!(connection_global_id)
        raise_unless_locked!

        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
            @server.address,
            Monitoring::Event::Cmap::ConnectionCheckOutFailed::TIMEOUT,
          ),
        )

        connection_global_id_msg = if connection_global_id
          " for connection #{connection_global_id}"
        else
          ''
        end

        msg = "Timed out attempting to check out a connection " +
          "from pool for #{@server.address}#{connection_global_id_msg} after #{wait_timeout} sec. " +
          "Connections in pool: #{@available_connections.length} available, " +
          "#{@checked_out_connections.length} checked out, " +
          "#{@pending_connections.length} pending, " +
          "#{@connection_requests} connections requests " +
          "(max size: #{max_size})"
        raise Error::ConnectionCheckOutTimeout.new(msg, address: @server.address)
      end

      def raise_check_out_timeout_locked!(connection_global_id)
        @lock.synchronize do
          raise_check_out_timeout!(connection_global_id)
        end
      end

      def raise_if_pool_closed!
        if closed?
          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
              @server.address,
              Monitoring::Event::Cmap::ConnectionCheckOutFailed::POOL_CLOSED
            ),
          )
          raise Error::PoolClosedError.new(@server.address, self)
        end
      end

      def raise_if_pool_paused!
        raise_unless_locked!

        if !@ready
          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
              @server.address,
              # CMAP spec decided to conflate pool paused with all the other
              # possible non-timeout errors.
              Monitoring::Event::Cmap::ConnectionCheckOutFailed::CONNECTION_ERROR,
            ),
          )
          raise Error::PoolPausedError.new(@server.address, self)
        end
      end

      def raise_if_pool_paused_locked!
        @lock.synchronize do
          raise_if_pool_paused!
        end
      end

      # The lock should be acquired when calling this method.
      def raise_if_not_ready!
        raise_unless_locked!
        raise_if_pool_closed!
        raise_if_pool_paused!
      end

      def raise_unless_locked!
        unless @lock.owned?
          raise ArgumentError, "the lock must be owned when calling this method"
        end
      end

      def valid_available_connection?(connection, pid, connection_global_id)
        if connection.pid != pid
          log_warn("Detected PID change - Mongo client should have been reconnected (old pid #{connection.pid}, new pid #{pid}")
          connection.disconnect!(reason: :stale)
          @populate_semaphore.signal
          return false
        end

        if !connection.pinned?
          # If connection is marked as pinned, it is used by a transaction
          # or a series of cursor operations in a load balanced setup.
          # In this case connection should not be disconnected until
          # unpinned.
          if connection.generation != generation(
            service_id: connection.service_id
          )
            # Stale connections should be disconnected in the clear
            # method, but if any don't, check again here
            connection.disconnect!(reason: :stale)
            @populate_semaphore.signal
            return false
          end

          if max_idle_time && connection.last_checkin &&
            Time.now - connection.last_checkin > max_idle_time
          then
            connection.disconnect!(reason: :idle)
            @populate_semaphore.signal
            return false
          end
        end
        true
      end

      # Retrieves a connection if one is available, otherwise we create a new
      # one. If no connection exists and the pool is at max size, wait until
      # a connection is checked back into the pool.
      #
      # @param [ Integer ] pid The current process id.
      # @param [ Integer ] connection_global_id The global id for the
      #   connection to check out.
      #
      # @return [ Mongo::Server::Connection ] The checked out connection.
      #
      # @raise [ Error::PoolClosedError ] If the pool has been closed.
      # @raise [ Timeout::Error ] If the connection pool is at maximum size
      #   and remains so for longer than the wait timeout.
      def get_connection(pid, connection_global_id)
        if connection = next_available_connection(connection_global_id)
          unless valid_available_connection?(connection, pid, connection_global_id)
            return nil
          end

          # We've got a connection, so we decrement the number of connection
          # requests.
          # We do not need to signal condition variable here, because
          # because the execution will continue, and we signal later.
          @connection_requests -= 1

          # If the connection is connected, it's not considered a
          # "pending connection". The pending_connections list represents
          # the set of connections that are awaiting connection.
          unless connection.connected?
            @pending_connections << connection
          end
          return connection
        elsif connection_global_id && @server.load_balancer?
          # A particular connection is requested, but it is not available.
          # If it is nether available not checked out, we should stop here.
          @checked_out_connections.detect do |conn|
            conn.global_id == connection_global_id
          end.tap do |conn|
            if conn.nil?
              publish_cmap_event(
                Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
                  @server.address,
                  Monitoring::Event::Cmap::ConnectionCheckOutFailed::CONNECTION_ERROR
                ),
              )
              # We're going to raise, so we need to decrement the number of
              # connection requests.
              decrement_connection_requests_and_signal
              raise Error::MissingConnection.new
            end
          end
          # We need a particular connection, and if it is not available
          # we can wait for an in-progress operation to return
          # such a connection to the pool.
          nil
        else
          connection = create_connection
          @connection_requests -= 1
          @pending_connections << connection
          return connection
        end
      end

      # Retrieves a connection and connects it.
      #
      # @param [ Integer ] connection_global_id The global id for the
      #   connection to check out.
      #
      # @return [ Mongo::Server::Connection ] The checked out connection.
      #
      # @raise [ Error::PoolClosedError ] If the pool has been closed.
      # @raise [ Timeout::Error ] If the connection pool is at maximum size
      #   and remains so for longer than the wait timeout.
      def retrieve_and_connect_connection(connection_global_id)
        deadline = Utils.monotonic_time + wait_timeout
        connection = nil

        @lock.synchronize do
          # The first gate to checking out a connection. Make sure the number of
          # unavailable connections is less than the max pool size.
          until max_size == 0 || unavailable_connections < max_size
            wait = deadline - Utils.monotonic_time
            raise_check_out_timeout!(connection_global_id) if wait <= 0
            @size_cv.wait(wait)
            raise_if_not_ready!
          end
          @connection_requests += 1
          connection = wait_for_connection(connection_global_id, deadline)
        end

        connect_or_raise(connection) unless connection.connected?

        @lock.synchronize do
          @checked_out_connections << connection
          if @pending_connections.include?(connection)
            @pending_connections.delete(connection)
          end
          @max_connecting_cv.signal
          # no need to signal size_cv here since the number of unavailable
          # connections is unchanged.
        end

        connection
      end

      # Waits for a connection to become available, or raises is no connection
      # becomes available before the timeout.
      # @param [ Integer ] connection_global_id The global id for the
      #   connection to check out.
      # @param [ Float ] deadline The time at which to stop waiting.
      #
      # @return [ Mongo::Server::Connection ] The checked out connection.
      def wait_for_connection(connection_global_id, deadline)
        connection = nil
        while connection.nil?
          # The second gate to checking out a connection. Make sure 1) there
          # exists an available connection and 2) we are under max_connecting.
          until @available_connections.any? || @pending_connections.length < @max_connecting
            wait = deadline - Utils.monotonic_time
            if wait <= 0
              # We are going to raise a timeout error, so the connection
              # request is not going to be fulfilled. Decrement the counter
              # here.
              decrement_connection_requests_and_signal
              raise_check_out_timeout!(connection_global_id)
            end
            @max_connecting_cv.wait(wait)
            # We do not need to decrement the connection_requests counter
            # or signal here because the pool is not ready yet.
            raise_if_not_ready!
          end

          connection = get_connection(Process.pid, connection_global_id)
          wait = deadline - Utils.monotonic_time
          if connection.nil? && wait <= 0
            # connection is nil here, it means that get_connection method
            # did not create a new connection; therefore, it did not decrease
            # the connection_requests counter. We need to do it here.
            decrement_connection_requests_and_signal
            raise_check_out_timeout!(connection_global_id)
          end
        end

        connection
      end

      # Connects a connection and raises an exception if the connection
      # cannot be connected.
      # This method also publish corresponding event and ensures that counters
      # and condition variables are updated.
      def connect_or_raise(connection)
        connect_connection(connection)
      rescue Exception
        # Handshake or authentication failed
        @lock.synchronize do
          if @pending_connections.include?(connection)
            @pending_connections.delete(connection)
          end
          @max_connecting_cv.signal
          @size_cv.signal
        end
        @populate_semaphore.signal
        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
            @server.address,
            Monitoring::Event::Cmap::ConnectionCheckOutFailed::CONNECTION_ERROR
          ),
        )
        raise
      end


      # Decrement connection requests counter and signal the condition
      # variables that the number of unavailable connections has decreased.
      def decrement_connection_requests_and_signal
        @connection_requests -= 1
        @max_connecting_cv.signal
        @size_cv.signal
      end
    end
  end
end

require 'mongo/server/connection_pool/generation_manager'
require 'mongo/server/connection_pool/populator'
