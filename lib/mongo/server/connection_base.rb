# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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

    # This class encapsulates common connection functionality.
    #
    # @note Although methods of this module are part of the public API,
    #   the fact that these methods are defined on this module and not on
    #   the classes which include this module is not part of the public API.
    #
    # @api semipublic
    class ConnectionBase < ConnectionCommon
      extend Forwardable
      include Monitoring::Publishable

      # The maximum allowed size in bytes that a user-supplied document may
      # take up when serialized, if the server's hello response does not
      # include maxBsonObjectSize field.
      #
      # The commands that are sent to the server may exceed this size by
      # MAX_BSON_COMMAND_OVERHEAD.
      #
      # @api private
      DEFAULT_MAX_BSON_OBJECT_SIZE = 16777216

      # The additional overhead allowed for command data (i.e. fields added
      # to the command document by the driver, as opposed to documents
      # provided by the user) when serializing a complete command to BSON.
      #
      # @api private
      MAX_BSON_COMMAND_OVERHEAD = 16384

      # @api private
      REDUCED_MAX_BSON_SIZE = 2097152

      # @return [ Hash ] options The passed in options.
      attr_reader :options

      # @return [ Server ] The server that this connection is for.
      #
      # @api private
      attr_reader :server

      # @return [ Mongo::Address ] address The address to connect to.
      def_delegators :server, :address

      # @deprecated
      def_delegators :server,
                     :cluster_time,
                     :update_cluster_time

      # Returns the server description for this connection, derived from
      # the hello response for the handshake performed on this connection.
      #
      # @note A connection object that hasn't yet connected (handshaken and
      #   authenticated, if authentication is required) does not have a
      #   description. While handshaking and authenticating the driver must
      #   be using global defaults, in particular not assuming that the
      #   properties of a particular connection are the same as properties
      #   of other connections made to the same address (since the server
      #   on the other end could have been shut down and a different server
      #   version could have been launched).
      #
      # @return [ Server::Description ] Server description for this connection.
      # @api private
      attr_reader :description

      # @deprecated
      def_delegators :description,
        :features,
        :max_bson_object_size,
        :max_message_size,
        :mongos?

      # @return [ nil | Object ] The service id, if any.
      def service_id
        description&.service_id
      end

      # Connection pool generation from which this connection was created.
      # May be nil.
      #
      # @return [ Integer | nil ] Connection pool generation.
      def generation
        # If the connection is to a load balancer, @generation is set
        # after handshake completes. If the connection is to another server
        # type, generation is specified during connection creation.
        @generation || options[:generation]
      end

      def app_metadata
        @app_metadata ||= begin
          same = true
          AppMetadata::AUTH_OPTION_KEYS.each do |key|
            if @server.options[key] != options[key]
              same = false
              break
            end
          end
          if same
            @server.app_metadata
          else
            AppMetadata.new(options.merge(purpose: @server.app_metadata.purpose))
          end
        end
      end

      # Dispatch a single message to the connection. If the message
      # requires a response, a reply will be returned.
      #
      # @example Dispatch the message.
      #   connection.dispatch([ insert ])
      #
      # @note This method is named dispatch since 'send' is a core Ruby method on
      #   all objects.
      #
      # @note For backwards compatibility, this method accepts the messages
      #   as an array. However, exactly one message must be given per invocation.
      #
      # @param [ Array<Message> ] messages A one-element array containing
      #   the message to dispatch.
      # @param [ Operation::Context ] context The operation context.
      # @param [ Hash ] options
      #
      # @option options [ Boolean ] :deserialize_as_bson Whether to deserialize
      #   the response to this message using BSON objects in place of native
      #   Ruby types wherever possible.
      #
      # @return [ Protocol::Message | nil ] The reply if needed.
      #
      # @raise [ Error::SocketError | Error::SocketTimeoutError ] When there is a network error.
      #
      # @since 2.0.0
      def dispatch(messages, context, options = {})
        # The monitoring code does not correctly handle multiple messages,
        # and the driver internally does not send more than one message at
        # a time ever. Thus prohibit multiple message use for now.
        if messages.length != 1
          raise ArgumentError, 'Can only dispatch one message at a time'
        end
        if description.unknown?
          raise Error::InternalDriverError, "Cannot dispatch a message on a connection with unknown description: #{description.inspect}"
        end
        message = messages.first
        deliver(message, context, options)
      end

      private

      # @raise [ Error::SocketError | Error::SocketTimeoutError ] When there is a network error.
      def deliver(message, context, options = {})
        if Lint.enabled? && !@socket
          raise Error::LintError, "Trying to deliver a message over a disconnected connection (to #{address})"
        end
        buffer = serialize(message, context)
        ensure_connected do |socket|
          operation_id = Monitoring.next_operation_id
          started_event = command_started(address, operation_id, message.payload,
            socket_object_id: socket.object_id, connection_id: id,
            connection_generation: generation,
            server_connection_id: description.server_connection_id,
            service_id: description.service_id,
          )
          start = Utils.monotonic_time
          result = nil
          begin
            result = add_server_diagnostics do
              socket.write(buffer.to_s)
              if message.replyable?
                Protocol::Message.deserialize(socket, max_message_size, message.request_id, options)
              else
                nil
              end
            end
          rescue Exception => e
            total_duration = Utils.monotonic_time - start
            command_failed(nil, address, operation_id, message.payload,
              e.message, total_duration,
              started_event: started_event,
              server_connection_id: description.server_connection_id,
              service_id: description.service_id,
            )
            raise
          else
            total_duration = Utils.monotonic_time - start
            command_completed(result, address, operation_id, message.payload,
              total_duration,
              started_event: started_event,
              server_connection_id: description.server_connection_id,
              service_id: description.service_id,
            )
          end
          if result && context.decrypt?
            result = result.maybe_decrypt(context)
          end
          result
        end
      end

      def serialize(message, context, buffer = BSON::ByteBuffer.new)
        # Driver specifications only mandate the fixed 16MiB limit for
        # serialized BSON documents. However, the server returns its
        # active serialized BSON document size limit in the hello response,
        # which is +max_bson_object_size+ below. The +DEFAULT_MAX_BSON_OBJECT_SIZE+
        # is the 16MiB value mandated by the specifications which we use
        # only as the default if the server's hello did not contain
        # maxBsonObjectSize.
        max_bson_size = max_bson_object_size || DEFAULT_MAX_BSON_OBJECT_SIZE
        if context.encrypt?
          # The client-side encryption specification requires bulk writes to
          # be split at a reduced maxBsonObjectSize. If this message is a bulk
          # write and its size exceeds the reduced size limit, the serializer
          # will raise an exception, which is caught by BulkWrite. BulkWrite
          # will split the operation into individual writes, which will
          # not be subject to the reduced maxBsonObjectSize.
          if message.bulk_write?
            # Make the new maximum size equal to the specified reduced size
            # limit plus the 16KiB overhead allowance.
            max_bson_size = REDUCED_MAX_BSON_SIZE
          end
        end

        # RUBY-2234: It is necessary to check that the message size does not
        # exceed the maximum bson object size before compressing and serializing
        # the final message.
        #
        # This is to avoid the case where the user performs a bulk write
        # larger than 16MiB which, when compressed, becomes smaller than 16MiB.
        # If the driver does not split the bulk writes prior to compression,
        # the entire operation will be sent to the server, which will raise an
        # error because the uncompressed operation exceeds the maximum bson size.
        #
        # To address this problem, we serialize the message prior to compression
        # and raise an exception if the serialized message exceeds the maximum
        # bson size.
        if max_message_size
          # Create a separate buffer that contains the un-compressed message
          # for the purpose of checking its size. Write any pre-existing contents
          # from the original buffer into the temporary one.
          temp_buffer = BSON::ByteBuffer.new

          # TODO: address the fact that this line mutates the buffer.
          temp_buffer.put_bytes(buffer.get_bytes(buffer.length))

          message.serialize(temp_buffer, max_bson_size, MAX_BSON_COMMAND_OVERHEAD)
          if temp_buffer.length > max_message_size
            raise Error::MaxMessageSize.new(max_message_size)
          end
        end

        # RUBY-2335: When the un-compressed message is smaller than the maximum
        # bson size limit, the message will be serialized twice. The operations
        # layer should be refactored to allow compression on an already-
        # serialized message.
        final_message = message.maybe_compress(compressor, options[:zlib_compression_level])
        final_message.serialize(buffer, max_bson_size, MAX_BSON_COMMAND_OVERHEAD)

        buffer
      end
    end
  end
end
