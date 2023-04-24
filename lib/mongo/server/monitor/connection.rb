# frozen_string_literal: true
# rubocop:todo all

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
        include Loggable

        # Creates a new connection object to the specified target address
        # with the specified options.
        #
        # The constructor does not perform any I/O (and thus does not create
        # sockets nor handshakes); call connect! method on the connection
        # object to create the network connection.
        #
        # @note Monitoring connections do not authenticate.
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
        #   driver only supports 'zstd', 'snappy' and 'zlib'.
        # @option options [ Float ] :connect_timeout The timeout, in seconds,
        #   to use for network operations. This timeout is used for all
        #   socket operations rather than connect calls only, contrary to
        #   what the name implies,
        #
        # @since 2.0.0
        def initialize(address, options = {})
          @address = address
          @options = options.dup.freeze
          unless @app_metadata = options[:app_metadata]
            raise ArgumentError, 'App metadata is required'
          end
          @socket = nil
          @pid = Process.pid
          @compressor = nil
          @hello_ok = false
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

        # @return [ Integer ] server_connection_id The server connection id.
        attr_reader :server_connection_id

        # Sends a message and returns the result.
        #
        # @param [ Protocol::Message ] message The message to send.
        #
        # @return [ Protocol::Message ] The result.
        def dispatch(message)
          dispatch_bytes(message.serialize.to_s)
        end

        # Sends a preserialized message and returns the result.
        #
        # @param [ String ] bytes The serialized message to send.
        #
        # @option opts [ Numeric ] :read_socket_timeout The timeout to use for
        #   each read operation.
        #
        # @return [ Protocol::Message ] The result.
        def dispatch_bytes(bytes, **opts)
          write_bytes(bytes)
          read_response(
            socket_timeout: opts[:read_socket_timeout],
          )
        end

        def write_bytes(bytes)
          unless connected?
            raise ArgumentError, "Trying to dispatch on an unconnected connection #{self}"
          end

          add_server_connection_id do
            add_server_diagnostics do
              socket.write(bytes)
            end
          end
        end

        # @option opts [ Numeric ] :socket_timeout The timeout to use for
        #   each read operation.
        def read_response(**opts)
          unless connected?
            raise ArgumentError, "Trying to read on an unconnected connection #{self}"
          end

          add_server_connection_id do
            add_server_diagnostics do
              Protocol::Message.deserialize(socket,
                Protocol::Message::MAX_MESSAGE_SIZE,
                nil,
                **opts)
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
          if @socket
            raise ArgumentError, 'Monitoring connection already connected'
          end

          @socket = add_server_diagnostics do
            address.socket(socket_timeout, ssl_options.merge(
              connection_address: address, monitor: true))
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
            socket.close rescue nil
            @socket = nil
          end
          true
        end

        # Send handshake command to connected host and validate the response.
        #
        # @return [BSON::Document] Handshake response from server
        #
        # @raise [Mongo::Error] If handshake failed.
        def handshake!
          command = handshake_command(
            handshake_document(
              @app_metadata,
              server_api: options[:server_api]
            )
          )
          payload = command.serialize.to_s
          message = dispatch_bytes(payload)
          result = Operation::Result.new(message)
          result.validate!
          reply = result.documents.first
          set_compressor!(reply)
          set_hello_ok!(reply)
          @server_connection_id = reply['connectionId']
          reply
        rescue => exc
          msg = "Failed to handshake with #{address}"
          Utils.warn_bg_exception(msg, exc,
            logger: options[:logger],
            log_prefix: options[:log_prefix],
            bg_error_backtrace: options[:bg_error_backtrace],
          )
          raise
        end

        # Build a document that should be used for connection check.
        #
        # @return [BSON::Document] Document that should be sent to a server
        #     for connection check.
        #
        # @api private
        def check_document
          server_api = @app_metadata.server_api || options[:server_api]
          doc = if hello_ok? || server_api
            _doc = HELLO_DOC
            if server_api
              _doc = _doc.merge(Utils.transform_server_api(server_api))
            end
            _doc
          else
            LEGACY_HELLO_DOC
          end
          # compressors must be set to maintain correct compression status
          # in the server description. See RUBY-2427
          if compressors = options[:compressors]
            doc = doc.merge(compression: compressors)
          end
          doc
        end

        private

        def add_server_connection_id
          yield
        rescue Mongo::Error => e
          if server_connection_id
            note = "sconn:#{server_connection_id}"
            e.add_note(note)
          end
          raise e
        end

        # Update @hello_ok flag according to server reply to legacy hello
        # command. The flag will be set to true if connected server supports
        # hello command, otherwise the flag will be set to false.
        #
        # @param [ BSON::Document ] reply Server reply to legacy hello command.
        def set_hello_ok!(reply)
          @hello_ok = !!reply[:helloOk]
        end

        def hello_ok?
          @hello_ok
        end
      end
    end
  end
end
