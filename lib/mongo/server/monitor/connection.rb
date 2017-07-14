# Copyright (C) 2015 MongoDB, Inc.

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
        ISMASTER_OP_MSG_MESSAGE = Protocol::Msg.new([:none], {}, ISMASTER_OP_MSG)

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
            read_with_one_retry do
              socket.write(ISMASTER_BYTES)
              Protocol::Message.deserialize(socket).documents[0]
            end
          end
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
          unless socket && socket.connectable?
            @socket = address.socket(socket_timeout, ssl_options)
            address.connect_socket!(socket)
            handshake!
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
          end
          true
        end

        # Initialize a new socket connection from the client to the server.
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
        # @since 2.0.0
        def initialize(address, options = {})
          @address = address
          @options = options.freeze
          @app_metadata = options[:app_metadata]
          @ssl_options = options.reject { |k, v| !k.to_s.start_with?(SSL) }
          @socket = nil
          @pid = Process.pid
          @compressor = nil
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

        def handshake!
          if @app_metadata
            socket.write(@app_metadata.ismaster_bytes)
            reply = Protocol::Message.deserialize(socket, Mongo::Protocol::Message::MAX_MESSAGE_SIZE).documents[0]
            set_compressor!(reply)
            reply
          end
        end
      end
    end
  end
end
