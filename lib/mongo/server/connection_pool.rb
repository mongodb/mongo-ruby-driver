# frozen_string_literal: true
# encoding: utf-8

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
      DEFAULT_MAX_SIZE = 20.freeze

      # The default min size for the connection pool.
      #
      # @since 2.9.0
      DEFAULT_MIN_SIZE = 0.freeze

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
        @closed = false

        # A connection owned by this pool should be either in the
        # available connections array (which is used as a stack)
        # or in the checked out connections set.
        @available_connections = available_connections = []
        @checked_out_connections = Set.new
        @pending_connections = Set.new

        # Mutex used for synchronizing access to @available_connections and
        # @checked_out_connections. The pool object is thread-safe, thus
        # all methods that retrieve or modify instance variables generally
        # must do so under this lock.
        @lock = Mutex.new

        # Condition variable broadcast when a connection is added to
        # @available_connections, to wake up any threads waiting for an
        # available connection when pool is at max size
        @available_semaphore = Semaphore.new

        # Background thread reponsible for maintaining the size of
        # the pool to at least min_size
        @populator = Populator.new(self, options)
        @populate_semaphore = Semaphore.new

        ObjectSpace.define_finalizer(self, self.class.finalize(@available_connections, @pending_connections, @populator))

        publish_cmap_event(
          Monitoring::Event::Cmap::PoolCreated.new(@server.address, options, self)
        )

        @populator.run! if min_size > 0
      end

      # @return [ Hash ] options The pool options.
      attr_reader :options

      # @api private
      def_delegators :@server, :address

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
      def_delegator :generation_manager, :generation

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

      # @note This method is experimental and subject to change.
      #
      # @api experimental
      # @since 2.11.0
      def summary
        @lock.synchronize do
          "#<ConnectionPool size=#{unsynchronized_size} (#{min_size}-#{max_size}) " +
            "used=#{@checked_out_connections.length} avail=#{@available_connections.length} pending=#{@pending_connections.length}>"
        end
      end

      # @since 2.9.0
      def_delegators :@server, :monitoring

      # @api private
      attr_reader :populator

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

        if closed?
          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
              @server.address,
              Monitoring::Event::Cmap::ConnectionCheckOutFailed::POOL_CLOSED
            ),
          )
          raise Error::PoolClosedError.new(@server.address, self)
        end

        deadline = Utils.monotonic_time + wait_timeout
        pid = Process.pid
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
                connection = next_available_connection(
                  connection_global_id: connection_global_id
                )

                if connection.nil?
                  if connection_global_id
                    # A particular connection is requested, but it is not available.
                    # If it is nether available not checked out, we should stop here.
                    @checked_out_connections.detect do |conn|
                      conn.connection_global_id == connection_global_id
                    end.tap do |conn|
                      if conn.nil?
                        publish_cmap_event(
                          Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
                            @server.address,
                            Monitoring::Event::Cmap::ConnectionCheckOutFailed::CONNECTION_ERROR
                          ),
                        )
                        raise Error::MissingConnection.new
                      end
                    end
                  else
                    break
                  end
                end

                if connection.pid != pid
                  log_warn("Detected PID change - Mongo client should have been reconnected (old pid #{connection.pid}, new pid #{pid}")
                  connection.disconnect!(reason: :stale)
                  @populate_semaphore.signal
                  next
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
                    next
                  end

                  if max_idle_time && connection.last_checkin &&
                    Time.now - connection.last_checkin > max_idle_time
                  then
                    connection.disconnect!(reason: :idle)
                    @populate_semaphore.signal
                    next
                  end
                end

                @pending_connections << connection
                throw(:done)
              end

              if @server.load_balancer? && connection_global_id
                # We need a  particular connection, and if it is not available
                # we can wait for an in-progress operation to return
                # such a connection to the pool.
              else
                # If we are below pool capacity, create a new connection.
                #
                # Ruby does not allow a thread to lock a mutex which it already
                # holds.
                if max_size == 0 || unsynchronized_size < max_size
                  connection = create_connection
                  @pending_connections << connection
                  throw(:done)
                end
              end
            end

            wait = deadline - Utils.monotonic_time
            if wait <= 0
              publish_cmap_event(
                Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(
                  @server.address,
                  Monitoring::Event::Cmap::ConnectionCheckOutFailed::TIMEOUT,
                ),
              )

              msg = @lock.synchronize do
                connection_global_id_msg = if connection_global_id
                  " for connection #{connection_global_id}"
                else
                  ''
                end

                "Timed out attempting to check out a connection " +
                  "from pool for #{@server.address}#{connection_global_id_msg} after #{wait_timeout} sec. " +
                  "Connections in pool: #{@available_connections.length} available, " +
                  "#{@checked_out_connections.length} checked out, " +
                  "#{@pending_connections.length} pending " +
                  "(max size: #{max_size})"
              end
              raise Error::ConnectionCheckOutTimeout.new(msg, address: @server.address)
            end
            @available_semaphore.wait(wait)
          end
        end

        begin
          connect_connection(connection)
        rescue Exception
          # Handshake or authentication failed
          @lock.synchronize do
            @pending_connections.delete(connection)
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

        @lock.synchronize do
          @checked_out_connections << connection
          @pending_connections.delete(connection)
        end

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
          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionCheckedIn.new(@server.address, connection.id, self)
          )

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

            # Wake up only one thread waiting for an available connection,
            # since only one connection was checked in.
            @available_semaphore.signal
          end
        end
      ensure
        check_invariants
      end

      # Closes all idle connections in the pool and schedules currently checked
      # out connections to be closed when they are checked back into the pool.
      # The pool remains operational and can create new connections when
      # requested.
      #
      # @option options [ true | false ] :lazy If true, do not close any of
      #   the idle connections and instead let them be closed during a
      #   subsequent check out operation.
      # @option options [ true | false ] :stop_populator Whether to stop
      #   the populator background thread. For internal driver use only.
      # @option options [ Object ] :service_id Clear connections with
      #   the specified service id only.
      #
      # @return [ true ] true.
      #
      # @since 2.1.0
      def clear(options = nil)
        raise_if_closed!

        check_invariants

        if options && options[:stop_populator]
          stop_populator
        end

        service_id = options && options[:service_id]

        @lock.synchronize do
          @generation_manager.bump(service_id: service_id)

          publish_cmap_event(
            Monitoring::Event::Cmap::PoolCleared.new(
              @server.address,
              service_id: service_id
            )
          )

          unless options && options[:lazy]
            if @server.load_balancer? && service_id
              loop do
                conn = @available_connections.detect do |conn|
                  conn.service_id == service_id
                end
                if conn
                  @available_connections.delete(conn)
                  conn.disconnect!(reason: :stale)
                  @populate_semaphore.signal
                else
                  break
                end
              end
            else
              until @available_connections.empty?
                connection = @available_connections.pop
                connection.disconnect!(reason: :stale)
                @populate_semaphore.signal
              end
            end
          end
        end

        true
      ensure
        check_invariants
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

          # mark pool as closed before releasing lock so
          # no connections can be created, checked in, or checked out
          @closed = true
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
          until @pending_connections.empty?
            connection = @pending_connections.take(1).first
            connection.disconnect!
            @pending_connections.delete(connection)
          end
        end
      end

      # Creates and adds a connection to the pool, if the pool's size is below
      # min_size. Retries once if a socket-related error is encountered during
      # this process and raises if a second error or a non socket-related error occurs.
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
      rescue Error::AuthError, Error
        # wake up one thread waiting for connections, since one could not
        # be created here, and can instead be created in flow
        @available_semaphore.signal
        raise
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
      def next_available_connection(connection_global_id: nil)
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
        opts = options.merge(
          connection_pool: self,
          # Do not pass app metadata - this will be retrieved by the connection
          # based on the auth needs.
        )
        unless @server.load_balancer?
          opts[:generation] = generation
        end
        connection = Connection.new(@server, opts)
      end

      # Create a connection, connect it, and add it to the pool.
      #
      # @return [ true | false ] True if a connection was created and
      #    added to the pool, false otherwise
      # @raise [ Mongo::Error ] An error encountered during connection connect
      def create_and_add_connection
        connection = nil

        @lock.synchronize do
          if !closed? && unsynchronized_size < min_size
            connection = create_connection
            @pending_connections << connection
          else
            return false
          end
        end

        begin
          connect_connection(connection)
        rescue Exception
          @lock.synchronize do
            @pending_connections.delete(connection)
          end
          raise
        end

        @lock.synchronize do
          @available_connections << connection
          @pending_connections.delete(connection)

          # wake up one thread waiting for connections, since one was created
          @available_semaphore.signal
        end

        true
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
    end
  end
end

require 'mongo/server/connection_pool/generation_manager'
require 'mongo/server/connection_pool/populator'
