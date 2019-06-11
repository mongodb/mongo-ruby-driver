# Copyright (C) 2015-2019 MongoDB, Inc.
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
      class Connection
        include Retryable
        include Connectable
        include Loggable

        # The command used for determining server status.
        #
        # @since 2.2.0
        ISMASTER = { :ismaster => 1 }.freeze

        # The command used for determining server status formatted for an OP_MSG (server versions >= 3.6).
        #
        # @since 2.5.0
        ISMASTER_OP_MSG = { :ismaster => 1, '$db' => Database::ADMIN }.freeze

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

        # The default time in seconds to timeout a connection attempt.
        #
        # @since 2.1.2
        #
        # @deprecated Please use Server::CONNECT_TIMEOUT instead. Will be removed in 3.0.0
        CONNECT_TIMEOUT = 10.freeze

        # Key for compression algorithms in the response from the server during handshake.
        #
        # @since 2.5.0
        COMPRESSION = 'compression'.freeze

        # Warning message that the server has no compression algorithms in common with those requested
        #   by the client.
        #
        # @since 2.5.0
        COMPRESSION_WARNING = 'The server has no compression algorithms in common with those requested. ' +
                                'Compression will not be used.'.freeze

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
          @options = options.freeze
          @app_metadata = options[:app_metadata]
          @socket = nil
          @pid = Process.pid
          @compressor = nil
        end

        # @return [ Hash ] options The passed in options.
        attr_reader :options

        # @return [ Mongo::Address ] address The address to connect to.
        attr_reader :address

        # The compressor, which is determined during the handshake.
        #
        # @since 2.5.0
        attr_reader :compressor

        # Send the preserialized ismaster call.
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
              socket.write(ISMASTER_BYTES)
              Protocol::Message.deserialize(socket).documents[0]
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
            socket = address.socket(socket_timeout, ssl_options,
              connect_timeout: address.connect_timeout)
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

        # Get the socket timeout.
        #
        # @example Get the socket timeout.
        #   connection.socket_timeout
        #
        # @return [ Float ] The socket timeout in seconds. Note that the Monitor's connection
        #  uses the connect timeout value for calling ismaster. See the Server Discovery and
        #  Monitoring specification for details.
        #
        # @since 2.4.3
        def socket_timeout
          @timeout ||= options[:connect_timeout] || Server::CONNECT_TIMEOUT
        end
        # @deprecated Please use :socket_timeout instead. Will be removed in 3.0.0
        alias :timeout :socket_timeout

        private

        def set_compressor!(reply)
          server_compressors = reply[COMPRESSION]

          if options[:compressors]
            if intersection = (server_compressors & options[:compressors])
              @compressor = intersection[0]
            else
              log_warn(COMPRESSION_WARNING)
            end
          end
        end

        def handshake!(socket)
          if @app_metadata
            socket.write(@app_metadata.ismaster_bytes)
            reply = Protocol::Message.deserialize(socket, Mongo::Protocol::Message::MAX_MESSAGE_SIZE).documents[0]
            set_compressor!(reply)
            reply
          end
        rescue => e
          log_warn("Failed to handshake with #{address}: #{e.class}: #{e}")
          raise
        end

        def retry_message
          "Retrying ismaster on #{address}"
        end
      end
    end
  end
end
