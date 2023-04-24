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

    # This class models the socket connections for servers and their behavior.
    #
    # @since 2.0.0
    class Connection < ConnectionBase
      include Monitoring::Publishable
      include Retryable
      include Id
      extend Forwardable

      # The ping command.
      #
      # @since 2.1.0
      #
      # @deprecated No longer necessary with Server Selection specification.
      PING = { :ping => 1 }.freeze

      # The ping command for an OP_MSG (server versions >= 3.6).
      #
      # @since 2.5.0
      #
      # @deprecated No longer necessary with Server Selection specification.
      PING_OP_MSG = { :ping => 1, '$db' => Database::ADMIN }.freeze

      # Ping message.
      #
      # @since 2.1.0
      #
      # @deprecated No longer necessary with Server Selection specification.
      PING_MESSAGE = Protocol::Query.new(Database::ADMIN, Database::COMMAND, PING, :limit => -1)

      # Ping message as an OP_MSG (server versions >= 3.6).
      #
      # @since 2.5.0
      #
      # @deprecated No longer necessary with Server Selection specification.
      PING_OP_MSG_MESSAGE = Protocol::Msg.new([], {}, PING_OP_MSG)

      # The ping message as raw bytes.
      #
      # @since 2.1.0
      #
      # @deprecated No longer necessary with Server Selection specification.
      PING_BYTES = PING_MESSAGE.serialize.to_s.freeze

      # The ping OP_MSG message as raw bytes (server versions >= 3.6).
      #
      # @since 2.5.0
      #
      # @deprecated No longer necessary with Server Selection specification.
      PING_OP_MSG_BYTES = PING_OP_MSG_MESSAGE.serialize.to_s.freeze

      # Creates a new connection object to the specified target address
      # with the specified options.
      #
      # The constructor does not perform any I/O (and thus does not create
      # sockets, handshakes nor authenticates); call connect! method on the
      # connection object to create the network connection.
      #
      # @api private
      #
      # @example Create the connection.
      #   Connection.new(server)
      #
      # @note Connection must never be directly instantiated outside of a
      #   Server.
      #
      # @param [ Mongo::Server ] server The server the connection is for.
      # @param [ Hash ] options The connection options.
      #
      # @option options :pipe [ IO ] The file descriptor for the read end of the
      #   pipe to listen on during the select system call when reading from the
      #   socket.
      # @option options [ Integer ] :generation The generation of this
      #   connection. The generation should only be specified in this option
      #   when not in load-balancing mode, and it should be the generation
      #   of the connection pool when the connection is created. In
      #   load-balancing mode, the generation is set on the connection
      #   after the handshake completes.
      # @option options [ Hash ] :server_api The requested server API version.
      #   This hash can have the following items:
      #   - *:version* -- string
      #   - *:strict* -- boolean
      #   - *:deprecation_errors* -- boolean
      #
      # @since 2.0.0
      def initialize(server, options = {})
        if server.load_balancer? && options[:generation]
          raise ArgumentError, "Generation cannot be set when server is a load balancer"
        end

        @id = server.next_connection_id
        @global_id = self.class.next_id
        @monitoring = server.monitoring
        @options = options.freeze
        @server = server
        @socket = nil
        @last_checkin = nil
        @auth_mechanism = nil
        @pid = Process.pid
        @pinned = false

        publish_cmap_event(
          Monitoring::Event::Cmap::ConnectionCreated.new(address, id)
        )
      end

      # @return [ Time ] The last time the connection was checked back into a pool.
      #
      # @since 2.5.0
      attr_reader :last_checkin

      # @return [ Integer ] The ID for the connection. This will be unique
      # across connections to the same server object.
      #
      # @since 2.9.0
      attr_reader :id

      # @return [ Integer ] The global ID for the connection. This will be unique
      # across all connections.
      attr_reader :global_id

      # The connection pool from which this connection was created.
      # May be nil.
      #
      # @api private
      def connection_pool
        options[:connection_pool]
      end

      # Whether the connection was connected and was not interrupted, closed,
      # or had an error raised.
      #
      # @return [ true | false ] if the connection was connected.
      def connected?
        !closed? && !error? && !interrupted? && !!@socket
      end

      # Whether the connection was closed.
      #
      # Closed connections should no longer be used. Instead obtain a new
      # connection from the connection pool.
      #
      # @return [ true | false ] Whether connection was closed.
      #
      # @since 2.9.0
      def closed?
        !!@closed
      end

      # Whether the connection was interrupted.
      #
      # Interrupted connections were already removed from the pool and should
      # not be checked back into the pool.
      #
      # @return [ true | false ] Whether connection was interrupted.
      def interrupted?
        !!@interrupted
      end

      # Mark the connection as interrupted.
      def interrupted!
        @interrupted = true
      end

      # @api private
      def error?
        !!@error
      end

      # Whether the connection is used by a transaction or cursor operations.
      #
      # Pinned connections should not be disconnected and removed from a
      # connection pool if they are idle or stale.
      #
      # # @return [ true | false ] Whether connection is pinned.
      #
      # @api private
      def pinned?
        @pinned
      end

      # Mark the connection as pinned.
      #
      # @api private
      def pin
        @pinned = true
      end

      # Mark the connection as not pinned.
      #
      # @api private
      def unpin
        @pinned = false
      end

      # Establishes a network connection to the target address.
      #
      # If the connection is already established, this method does nothing.
      #
      # @example Connect to the host.
      #   connection.connect!
      #
      # @note This method mutates the connection object by setting a socket if
      #   one previously did not exist.
      #
      # @return [ true ] If the connection succeeded.
      #
      # @since 2.0.0
      def connect!
        raise_if_closed!

        unless @socket
          @socket = create_socket
          @description, @compressor = do_connect

          if server.load_balancer?
            if Lint.enabled?
              unless service_id
                raise Error::InternalDriverError, "The connection is to a load balancer and it must have service_id set here, but does not"
              end
            end
            @generation = connection_pool.generation_manager.generation(service_id: service_id)
          end

          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionReady.new(address, id)
          )

          @close_event_published = false
        end
        true
      end

      # Creates the socket. The method is separate from do_connect, so that
      # pending connections can be closed if they are interrupted during hello.
      #
      #
      # @return [ Socket ] The created socket.
      private def create_socket
        add_server_diagnostics do
          address.socket(socket_timeout, ssl_options.merge(
            connection_address: address, connection_generation: generation, pipe: options[:pipe]))
        end
      end

      # Separate method to permit easier mocking in the test suite.
      #
      # @return [ Array<Server::Description, String | Symbol> ] A server
      #   description instance from the hello response of the returned socket
      #   and the compressor to use.
      private def do_connect
        raise_if_closed!
        begin
          pending_connection = PendingConnection.new(
            socket, @server, monitoring, options.merge(id: id))
          pending_connection.handshake_and_authenticate!
        rescue Exception
          socket&.close
          @socket = nil
          raise
        end

        [pending_connection.description, pending_connection.compressor]
      end

      # Disconnect the connection.
      #
      # @note Once a connection is disconnected, it should no longer be used.
      #   A new connection should be obtained from the connection pool which
      #   will either return a ready connection or create a new connection.
      #   If linting is enabled, reusing a disconnected connection will raise
      #   Error::LintError. If linting is not enabled, a warning will be logged.
      #
      # @note This method mutates the connection object by setting the socket
      #   to nil if the closing succeeded.
      #
      # @option options [ Symbol ] :reason The reason why the connection is
      #   being closed.
      # @option options [ true | false ] :interrupted Whether or not the
      #   connection was interrupted.
      #
      # @return [ true ] If the disconnect succeeded.
      #
      # @since 2.0.0
      def disconnect!(options = nil)
        # Note: @closed may be true here but we also may have a socket.
        # Check the socket and not @closed flag.
        @auth_mechanism = nil
        @last_checkin = nil
        if socket
          socket.close rescue nil
          @socket = nil
        end
        @closed = true
        interrupted! if options && options[:interrupted]

        # To satisfy CMAP spec tests, publish close events even if the
        # socket was never connected (and thus the ready event was never
        # published). But track whether we published close event and do not
        # publish it multiple times, unless the socket was reconnected -
        # in that case publish the close event once per socket close.
        unless @close_event_published
          reason = options && options[:reason]
          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionClosed.new(
              address,
              id,
              reason,
            ),
          )
          @close_event_published = true
        end

        true
      end

      # Ping the connection to see if the server is responding to commands.
      # This is non-blocking on the server side.
      #
      # @example Ping the connection.
      #   connection.ping
      #
      # @note This uses a pre-serialized ping message for optimization.
      #
      # @return [ true, false ] If the server is accepting connections.
      #
      # @since 2.1.0
      #
      # @deprecated No longer necessary with Server Selection specification.
      def ping
        bytes = features.op_msg_enabled? ? PING_OP_MSG_BYTES : PING_BYTES
        ensure_connected do |socket|
          reply = add_server_diagnostics do
            socket.write(bytes)
            Protocol::Message.deserialize(socket, max_message_size)
          end
          reply.documents[0][Operation::Result::OK] == 1
        end
      end

      # Get the timeout to execute an operation on a socket.
      #
      # @return [ Float ] The operation timeout in seconds.
      #
      # @since 2.0.0
      def socket_timeout
        @timeout ||= options[:socket_timeout]
      end
      # @deprecated Please use :socket_timeout instead. Will be removed in 3.0.0
      alias :timeout :socket_timeout

      # Record the last checkin time.
      #
      # @example Record the checkin time on this connection.
      #   connection.record_checkin!
      #
      # @return [ self ]
      #
      # @since 2.5.0
      def record_checkin!
        @last_checkin = Time.now
        self
      end

      private

      def deliver(message, client, options = {})
        handle_errors do
          super
        end
      end

      def handle_errors
        begin
          yield
        rescue Error::SocketError => e
          @error = e
          @server.unknown!(
            generation: e.generation,
            # or description.service_id?
            service_id: e.service_id,
            stop_push_monitor: true,
          )
          raise
        rescue Error::SocketTimeoutError => e
          @error = e
          raise
        end
      end

      def raise_if_closed!
        if error?
          raise Error::ConnectionPerished, "Connection #{generation}:#{id} for #{address.seed} is perished. Reconnecting closed or errored connections is no longer supported"
        end

        if closed?
          raise Error::ConnectionPerished, "Connection #{generation}:#{id} for #{address.seed} is closed. Reconnecting closed or errored connections is no longer supported"
        end
      end
    end
  end
end
