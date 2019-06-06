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

    # This class models the socket connections for servers and their behavior.
    #
    # @since 2.0.0
    class Connection < ConnectionBase
      include Connectable
      include Monitoring::Publishable
      include Retryable
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
      # @option options [ Integer ] :generation Connection pool's generation
      #   for this connection.
      #
      # @since 2.0.0
      def initialize(server, options = {})
        @id = server.next_connection_id
        @monitoring = server.monitoring
        @options = options.freeze
        @server = server
        @socket = nil
        @last_checkin = nil
        @auth_mechanism = nil
        @pid = Process.pid

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

      # Connection pool generation from which this connection was created.
      # May be nil.
      #
      # @since 2.7.0
      # @api private
      def generation
        options[:generation]
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
        if closed?
          if Lint.enabled?
            raise Error::LintError, "Reconnecting closed connections is no longer supported"
          else
            log_warn("Reconnecting closed connections is deprecated (for #{address})")
          end
        end

        unless @socket
          # When @socket is assigned, the socket should have handshaken and
          # authenticated and be usable.
          @socket = do_connect

          publish_cmap_event(
            Monitoring::Event::Cmap::ConnectionReady.new(address, id)
          )

          @close_event_published = false
        end
        true
      end

      # Separate method to permit easier mocking in the test suite.
      def do_connect
        socket = address.socket(socket_timeout, ssl_options,
          connect_timeout: address.connect_timeout)
        handshake!(socket)
        pending_connection = PendingConnection.new(socket, @server, monitoring, options)
        authenticate!(pending_connection)
        socket
      end
      private :do_connect

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
          socket.close
          @socket = nil
        end
        @closed = true

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
          socket.write(bytes)
          reply = Protocol::Message.deserialize(socket, max_message_size)
          reply.documents[0][Operation::Result::OK] == 1
        end
      end

      # Get the timeout to execute an operation on a socket.
      #
      # @example Get the timeout to execute an operation on a socket.
      #   connection.timeout
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

      def handshake!(socket)
        unless socket
          raise Error::HandshakeError, "Cannot handshake because there is no usable socket"
        end

        response = average_rtt = nil
        @server.handle_handshake_failure! do
          begin
            response, exc, rtt, average_rtt =
              @server.monitor.round_trip_time_averager.measure do
                socket.write(app_metadata.ismaster_bytes)
                Protocol::Message.deserialize(socket, max_message_size).documents[0]
              end

            if exc
              raise exc
            end
          rescue => e
            log_warn("Failed to handshake with #{address}: #{e.class}: #{e}")
            raise
          end
        end

        post_handshake(response, average_rtt)
      end

      # This is a separate method to keep the nesting level down.
      def post_handshake(response, average_rtt)
        if response["ok"] == 1
          # Auth mechanism is entirely dependent on the contents of
          # ismaster response *for this connection*.
          # Ismaster received by the monitoring connection should advertise
          # the same wire protocol, but if it doesn't, we use whatever
          # the monitoring connection advertised for filling out the
          # server description and whatever the non-monitoring connection
          # (that's this one) advertised for performing auth on that
          # connection.
          @auth_mechanism = if response['saslSupportedMechs']
            if response['saslSupportedMechs'].include?(Mongo::Auth::SCRAM::SCRAM_SHA_256_MECHANISM)
              :scram256
            else
              :scram
            end
          else
            # MongoDB servers < 2.6 are no longer suported.
            # Wire versions should always be returned in ismaster.
            # See also https://jira.mongodb.org/browse/RUBY-1584.
            min_wire_version = response[Description::MIN_WIRE_VERSION]
            max_wire_version = response[Description::MAX_WIRE_VERSION]
            features = Description::Features.new(min_wire_version..max_wire_version)
            if features.scram_sha_1_enabled?
              :scram
            else
              :mongodb_cr
            end
          end
        else
          @auth_mechanism = nil
        end

        new_description = Description.new(address, response, average_rtt)
        @server.monitor.publish(Event::DESCRIPTION_CHANGED, @server.description, new_description)
      end

      def authenticate!(pending_connection)
        if options[:user] || options[:auth_mech]
          user = Auth::User.new(Options::Redacted.new(:auth_mech => default_mechanism).merge(options))
          @server.handle_auth_failure! do
            begin
              Auth.get(user).login(pending_connection)
            rescue => e
              log_warn("Failed to handshake with #{address}: #{e.class}: #{e}")
              raise
            end
          end
        end
      end

      def default_mechanism
        @auth_mechanism || (@server.features.scram_sha_1_enabled? ? :scram : :mongodb_cr)
      end

      def deliver(message)
        begin
          super
        # Important: timeout errors are not handled here
        rescue Error::SocketError
          @server.unknown!
          @server.pool.disconnect!
          raise
        end
      end
    end
  end
end
