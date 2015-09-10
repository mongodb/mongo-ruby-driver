# Copyright (C) 2014-2015 MongoDB, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

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
    class Connection
      include Connectable
      include Monitoring::Publishable
      extend Forwardable

      # The ping command.
      #
      # @since 2.1.0
      PING = { :ping => 1 }.freeze

      # Ping message.
      #
      # @since 2.1.0
      PING_MESSAGE = Protocol::Query.new(Database::ADMIN, Database::COMMAND, PING, :limit => -1)

      # The ping message as raw bytes.
      #
      # @since 2.1.0
      PING_BYTES = PING_MESSAGE.serialize.freeze

      # @return [ Mongo::Auth::CR, Mongo::Auth::X509, Mongo::Auth:LDAP, Mongo::Auth::SCRAM ]
      #   authenticator The authentication strategy.
      attr_reader :authenticator

      def_delegators :@server,
                     :features,
                     :max_bson_object_size,
                     :max_message_size,
                     :mongos?

      # Is this connection authenticated. Will return true if authorization
      # details were provided and authentication passed.
      #
      # @example Is the connection authenticated?
      #   connection.authenticated?
      #
      # @return [ true, false ] If the connection is authenticated.
      #
      # @since 2.0.0
      def authenticated?
        !!@authenticated
      end

      # Tell the underlying socket to establish a connection to the host.
      #
      # @example Connect to the host.
      #   connection.connect!
      #
      # @note This method mutates the connection class by setting a socket if
      #   one previously did not exist.
      #
      # @return [ true ] If the connection succeeded.
      #
      # @since 2.0.0
      def connect!
        unless socket
          @socket = address.socket(timeout, ssl_options)
          socket.connect!
          if authenticator
            authenticator.login(self)
            @authenticated = true
          end
        end
        true
      end

      # Disconnect the connection.
      #
      # @example Disconnect from the host.
      #   connection.disconnect!
      #
      # @note This method mutates the connection by setting the socket to nil
      #   if the closing succeeded.
      #
      # @return [ true ] If the disconnect succeeded.
      #
      # @since 2.0.0
      def disconnect!
        if socket
          socket.close
          @socket = nil
          @authenticated = false
        end
        true
      end

      # Dispatch the provided messages to the connection. If the last message
      # requires a response a reply will be returned.
      #
      # @example Dispatch the messages.
      #   connection.dispatch([ insert, command ])
      #
      # @note This method is named dispatch since 'send' is a core Ruby method on
      #   all objects.
      #
      # @param [ Array<Message> ] messages The messages to dispatch.
      # @param [ Integer ] operation_id The operation id to link messages.
      #
      # @return [ Protocol::Reply ] The reply if needed.
      #
      # @since 2.0.0
      def dispatch(messages, operation_id = nil)
        if monitoring.subscribers?(Monitoring::COMMAND)
          publish_command(messages, operation_id || Monitoring.next_operation_id) do |msgs|
            deliver(msgs)
          end
        else
          deliver(messages)
        end
      end

      # Initialize a new socket connection from the client to the server.
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
      # @since 2.0.0
      def initialize(server, options = {})
        @address = server.address
        @monitoring = server.monitoring
        @options = options.freeze
        @server = server
        @ssl_options = options.reject { |k, v| !k.to_s.start_with?(SSL) }
        @socket = nil
        @pid = Process.pid
        setup_authentication!
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
      def ping
        ensure_connected do |socket|
          socket.write(PING_BYTES)
          reply = Protocol::Reply.deserialize(socket)
          reply.documents[0][Operation::Result::OK] == 1
        end
      end

      private

      def deliver(messages)
        write(messages)
        messages.last.replyable? ? read : nil
      end

      def setup_authentication!
        if options[:user]
          default_mechanism = @server.features.scram_sha_1_enabled? ? :scram : :mongodb_cr
          user = Auth::User.new(Options::Redacted.new(:auth_mech => default_mechanism).merge(options))
          @authenticator = Auth.get(user)
        end
      end

      def write(messages, buffer = ''.force_encoding(BSON::BINARY))
        start_size = 0
        messages.each do |message|
          message.serialize(buffer, max_bson_object_size)
          if max_message_size &&
            (buffer.size - start_size) > max_message_size
            raise Error::MaxMessageSize.new(max_message_size)
            start_size = buffer.size
          end
        end
        ensure_connected{ |socket| socket.write(buffer) }
      end
    end
  end
end
