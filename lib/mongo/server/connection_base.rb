# Copyright (C) 2019 MongoDB, Inc.
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
    class ConnectionBase
      extend Forwardable
      include Monitoring::Publishable

      # @return [ Hash ] options The passed in options.
      attr_reader :options

      # @return [ Mongo::Address ] address The address to connect to.
      def address
        @server.address
      end

      def_delegators :@server,
                     :features,
                     :max_bson_object_size,
                     :max_message_size,
                     :mongos?,
                     :compressor,
                     :cluster_time,
                     :update_cluster_time

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
            AppMetadata.new(options)
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
      # @param [ Integer ] operation_id The operation id to link messages.
      #
      # @return [ Protocol::Message | nil ] The reply if needed.
      #
      # @since 2.0.0
      def dispatch(messages, operation_id = nil)
        # The monitoring code does not correctly handle multiple messages,
        # and the driver internally does not send more than one message at
        # a time ever. Thus prohibit multiple message use for now.
        if messages.length != 1
          raise ArgumentError, 'Can only dispatch one message at a time'
        end
        message = messages.first
        deliver(message)
      end

      private

      def deliver(message)
        buffer = serialize(message)
        ensure_connected do |socket|
          operation_id = Monitoring.next_operation_id
          command_started(address, operation_id, message.payload, socket.object_id)
          start = Time.now
          result = nil
          begin
            socket.write(buffer.to_s)
            result = if message.replyable?
              Protocol::Message.deserialize(socket, max_message_size, message.request_id)
            else
              nil
            end
          rescue Exception => e
            total_duration = Time.now - start
            command_failed(nil, address, operation_id, message.payload, e.message, total_duration)
            raise
          else
            total_duration = Time.now - start
            command_completed(result, address, operation_id, message.payload, total_duration)
          end
          result
        end
      end

      def serialize(message, buffer = BSON::ByteBuffer.new)
        start_size = 0
        message.compress!(compressor, options[:zlib_compression_level]).serialize(buffer, max_bson_object_size)
        if max_message_size &&
          (buffer.length - start_size) > max_message_size
        then
          raise Error::MaxMessageSize.new(max_message_size)
        end
        buffer
      end
    end
  end
end
