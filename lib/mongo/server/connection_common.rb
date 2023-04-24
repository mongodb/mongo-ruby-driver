# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2020 MongoDB Inc.
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

    # Common methods used by both monitoring and non-monitoring connections.
    #
    # @note Although methods of this module are part of the public API,
    #   the fact that these methods are defined on this module and not on
    #   the classes which include this module is not part of the public API.
    #
    # @api semipublic
    class ConnectionCommon

      # The compressor negotiated during the handshake for this connection,
      # if any.
      #
      # This attribute is nil for connections that haven't completed the
      # handshake yet, and for connections that negotiated no compression.
      #
      # @return [ String | nil ] The compressor.
      attr_reader :compressor

      # Determine if the connection is currently connected.
      #
      # @example Is the connection connected?
      #   connection.connected?
      #
      # @return [ true, false ] If connected.
      #
      # @deprecated
      def connected?
        !!socket
      end

      # @return [ Integer ] pid The process id when the connection was created.
      # @api private
      attr_reader :pid

      # Build a document that should be used for connection handshake.
      #
      # @param [ Server::AppMetadata ] app_metadata Application metadata
      # @param [ BSON::Document ] speculative_auth_doc The speculative
      #   authentication document, if any.
      # @param [ true | false ] load_balancer Whether the connection is to
      #   a load balancer.
      # @param server_api [ Hash | nil ] server_api Server API version.
      #
      # @return [BSON::Document] Document that should be sent to a server
      #     for handshake purposes.
      #
      # @api private
      def handshake_document(app_metadata, speculative_auth_doc: nil, load_balancer: false, server_api: nil)
        serv_api = app_metadata.server_api || server_api
        document = if serv_api
                     HELLO_DOC.merge(Utils.transform_server_api(serv_api))
                   else
                     LEGACY_HELLO_DOC
                   end
        document.merge(app_metadata.validated_document).tap do |doc|
          if speculative_auth_doc
            doc.update(speculativeAuthenticate: speculative_auth_doc)
          end
          if load_balancer
            doc.update(loadBalanced: true)
          end
        end
      end

      # Build a command that should be used for connection handshake.
      #
      # @param [ BSON::Document ] handshake_document Document that should be
      #   sent to a server for handshake purpose.
      #
      # @return [ Protocol::Message ] Command that should be sent to a server
      #   for handshake purposes.
      #
      # @api private
      def handshake_command(handshake_document)
        if handshake_document['apiVersion'] || handshake_document['loadBalanced']
          Protocol::Msg.new(
            [], {}, handshake_document.merge({'$db' => Database::ADMIN})
          )
        else
          Protocol::Query.new(
            Database::ADMIN,
            Database::COMMAND,
            handshake_document,
            :limit => -1
          )
        end
      end


      private

      HELLO_DOC = BSON::Document.new({ hello: 1 }).freeze

      LEGACY_HELLO_DOC = BSON::Document.new({ isMaster: 1, helloOk: true }).freeze

      attr_reader :socket

      def set_compressor!(reply)
        server_compressors = reply['compression']

        if options[:compressors]
          if intersection = (server_compressors & options[:compressors])
            @compressor = intersection.first
          else
            msg = if server_compressors
              "The server at #{address} has no compression algorithms in common with those requested. " +
                "Server algorithms: #{server_compressors.join(', ')}; " +
                "Requested algorithms: #{options[:compressors].join(', ')}. " +
                "Compression will not be used"
            else
              "The server at #{address} did not advertise compression support. " +
                "Requested algorithms: #{options[:compressors].join(', ')}. " +
                "Compression will not be used"
            end
            log_warn(msg)
          end
        end
      end

      # Yields to the block and, if the block raises an exception, adds a note
      # to the exception with the address of the specified server.
      #
      # This method is intended to add server address information to exceptions
      # raised during execution of operations on servers.
      def add_server_diagnostics
        yield
      # Note that the exception should already have been mapped to a
      # Mongo::Error subclass when it gets to this method.
      rescue Error::SocketError, Error::SocketTimeoutError => e
        # Server::Monitor::Connection does not reference its server, but
        # knows its address. Server::Connection delegates the address to its
        # server.
        note = +"on #{address.seed}"
        if respond_to?(:id)
          note << ", connection #{generation}:#{id}"
        end
        # Non-monitoring connections have service id.
        # Monitoring connections do not.
        if respond_to?(:service_id) && service_id
          note << ", service id #{service_id}"
        end
        e.add_note(note)
        if respond_to?(:generation)
          # Non-monitoring connections
          e.generation = generation
          if respond_to?(:global_id)
            e.connection_global_id = global_id
          end
          if respond_to?(:description)
            e.service_id = service_id
          end
        end
        raise e
      end

      def ssl_options
        @ssl_options ||= if options[:ssl]
          options.select { |k, v| k.to_s.start_with?('ssl') }
        else
          # Due to the way options are propagated from the client, if we
          # decide that we don't want to use TLS we need to have the :ssl
          # option explicitly set to false or the value provided to the
          # connection might be overwritten by the default inherited from
          # the client.
          {ssl: false}
        end.freeze
      end

      def ensure_connected
        begin
          unless socket
            raise ArgumentError, "Connection #{generation}:#{id} for #{address.seed} is not connected"
          end
          if @error
            raise Error::ConnectionPerished, "Connection #{generation}:#{id} for #{address.seed} is perished"
          end
          result = yield socket
          success = true
          result
        ensure
          unless success
            @error = true
          end
        end
      end
    end
  end
end
