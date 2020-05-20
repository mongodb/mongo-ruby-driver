# Copyright (C) 2015-2020 MongoDB Inc.
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
    class Monitor

      # This class models the monitor connections and their behavior.
      #
      # @since 2.0.0
      # @api private
      class Connection < Server::ConnectionCommon
        include Retryable
        include Loggable

        # The command used for determining server status.
        #
        # The case matters here for fail points.
        #
        # @since 2.2.0
        ISMASTER = { isMaster: 1 }.freeze

        # The command used for determining server status formatted for an
        # OP_MSG (server versions >= 3.6).
        #
        # The case matters here for fail points.
        #
        # @since 2.5.0
        ISMASTER_OP_MSG = { isMaster: 1, '$db' => Database::ADMIN }.freeze

        # The constant for the ismaster command.
        #
        # @since 2.2.0
        ISMASTER_MESSAGE = Protocol::Query.new(Database::ADMIN, Database::COMMAND, ISMASTER, :limit => -1)

        # The constant for the ismaster command as an OP_MSG (server versions >= 3.6).
        #
        # @since 2.5.0
        ISMASTER_OP_MSG_MESSAGE = Protocol::Msg.new([], {}, ISMASTER_OP_MSG)

        # The raw bytes for the ismaster message.
        #
        # @since 2.2.0
        ISMASTER_BYTES = ISMASTER_MESSAGE.serialize.to_s.freeze

        # The raw bytes for the ismaster OP_MSG message (server versions >= 3.6).
        #
        # @since 2.5.0
        ISMASTER_OP_MSG_BYTES = ISMASTER_OP_MSG_MESSAGE.serialize.to_s.freeze

        # Creates a new connection object to the specified target address
        # with the specified options.
        #
        # The constructor does not perform any I/O (and thus does not create
        # sockets nor handshakes); call connect! method on the connection
        # object to create the network connection.
        #
        # @note Monitoring connections do not authenticate.
        #
        # @api private
        #
        # @example Create the connection.
        #   Connection.new(address)
        #
        # @note Connection must never be directly instantiated outside of a
        #   Monitor.
        #
        # @param [ Mongo::Address ] address The address the connection is for.
        # @param [ Hash ] options The connection options.
        #
        # @option options [ Mongo::Server::Monitor::AppMetadata ] :app_metadata
        #   Metadata to use for handshake. If missing or nil, handshake will
        #   not be performed. Although a Mongo::Server::AppMetadata instance
        #   will also work, monitoring connections are meant to use
        #   Mongo::Server::Monitor::AppMetadata instances in order to omit
        #   performing SCRAM negotiation with the server, as monitoring
        #   sockets do not authenticate.
        # @option options [ Array<String> ] :compressors A list of potential
        #   compressors to use, in order of preference. The driver chooses the
        #   first compressor that is also supported by the server. Currently the
        #   driver only supports 'zlib'.
        # @option options [ Float ] :connect_timeout The timeout, in seconds,
        #   to use for network operations. This timeout is used for all
        #   socket operations rather than connect calls only, contrary to
        #   what the name implies,
        #
        # @since 2.0.0
        def initialize(address, options = {})
          @address = address
          @options = options.dup.freeze
          @app_metadata = options[:app_metadata]
          @socket = nil
          @pid = Process.pid
          @compressor = nil
        end

        # @return [ Hash ] options The passed in options.
        attr_reader :options

        # @return [ Mongo::Address ] address The address to connect to.
        attr_reader :address

        # Returns the monitoring socket timeout.
        #
        # Note that monitoring connections use the connect timeout value as
        # the socket timeout value. See the Server Discovery and Monitoring
        # specification for details.
        #
        # @return [ Float ] The socket timeout in seconds.
        #
        # @since 2.4.3
        def socket_timeout
          options[:connect_timeout] || Server::CONNECT_TIMEOUT
        end

        # Sends the preserialized ismaster request and returns the result.
        #
        # If there is any error during the ismaster request (such as a network
        # error), the request is retried.
        #
        # @example Send a preserialized ismaster message.
        #   connection.ismaster
        #
        # @return [ BSON::Document ] The ismaster result.
        #
        # @since 2.2.0
        def ismaster
          ensure_connected do |socket|
            read_with_one_retry(retry_message: retry_message) do
              add_server_diagnostics do
                socket.write(ISMASTER_BYTES)
                Protocol::Message.deserialize(socket).documents[0]
              end
            end
          end
        end

        # Establishes a network connection to the target address.
        #
        # If the connection is already established, this method does nothing.
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
          unless @socket
            socket = add_server_diagnostics do
              address.socket(socket_timeout, ssl_options, address.options)
            end
            handshake!(socket)
            @socket = socket
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
        # @note This method accepts an options argument for compatibility with
        #   Server::Connections. However, all options are ignored.
        #
        # @return [ true ] If the disconnect succeeded.
        #
        # @since 2.0.0
        def disconnect!(options = nil)
          if socket
            socket.close
            @socket = nil
          end
          true
        end

        private

        def handshake!(socket)
          payload = if @app_metadata
            @app_metadata.ismaster_bytes
          else
            log_warn("No app metadata provided for handshake with #{address}")
            ISMASTER_BYTES
          end
          reply = add_server_diagnostics do
            socket.write(payload)
            msg = Protocol::Message.deserialize(socket, Mongo::Protocol::Message::MAX_MESSAGE_SIZE)
            msg.documents.first
          end
          set_compressor!(reply)
          reply
        rescue => e
          log_warn("Failed to handshake with #{address}: #{e.class}: #{e}:\n#{e.backtrace[0..5].join("\n")}")
          raise
        end

        def retry_message
          "Retrying ismaster in monitor for #{address}"
        end

        def ensure_connected
          if pid != Process.pid
            log_warn("Detected PID change - Mongo client should have been reconnected (old pid #{pid}, new pid #{Process.pid}")
            disconnect!
            connect!
            @pid = Process.pid
          end

          super
        end
      end
    end
  end
end
