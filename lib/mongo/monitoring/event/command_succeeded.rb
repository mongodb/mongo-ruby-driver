# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Monitoring
    module Event

      # Event that is fired when a command operation succeeds.
      #
      # @since 2.1.0
      class CommandSucceeded < Mongo::Event::Base
        include Secure

        # @return [ Server::Address ] address The server address.
        attr_reader :address

        # @return [ String ] command_name The name of the command.
        attr_reader :command_name

        # @return [ BSON::Document ] reply The command reply.
        attr_reader :reply

        # @return [ String ] database_name The name of the database.
        attr_reader :database_name

        # @return [ Float ] duration The duration of the event.
        attr_reader :duration

        # @return [ Integer ] operation_id The operation id.
        attr_reader :operation_id

        # @return [ Integer ] request_id The request id.
        attr_reader :request_id

        # @return [ Integer ] server_connection_id The server connection id.
        attr_reader :server_connection_id

        # @return [ nil | Object ] The service id, if any.
        attr_reader :service_id

        # @return [ Monitoring::Event::CommandStarted ] started_event The corresponding
        #   started event.
        #
        # @api private
        attr_reader :started_event

        # Create the new event.
        #
        # @example Create the event.
        #
        # @param [ String ] command_name The name of the command.
        # @param [ String ] database_name The database name.
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] request_id The request id.
        # @param [ Integer ] operation_id The operation id.
        # @param [ BSON::Document ] reply The command reply.
        # @param [ Float ] duration The duration the command took in seconds.
        # @param [ Monitoring::Event::CommandStarted ] started_event The corresponding
        #   started event.
        # @param [ Object ] service_id The service id, if any.
        #
        # @since 2.1.0
        # @api private
        def initialize(command_name, database_name, address, request_id,
          operation_id, reply, duration, started_event:,
          server_connection_id: nil, service_id: nil
        )
          @command_name = command_name.to_s
          @database_name = database_name
          @address = address
          @request_id = request_id
          @operation_id = operation_id
          @service_id = service_id
          @started_event = started_event
          @reply = redacted(command_name, reply)
          @duration = duration
          @server_connection_id = server_connection_id
        end

        # Returns a concise yet useful summary of the event.
        #
        # @return [ String ] String summary of the event.
        #
        # @note This method is experimental and subject to change.
        #
        # @api experimental
        def summary
          "#<#{short_class_name} address=#{address} #{database_name}.#{command_name}>"
        end

        # Create the event from a wire protocol message payload.
        #
        # @example Create the event.
        #   CommandSucceeded.generate(address, 1, command_payload, reply_payload, 0.5)
        #
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] operation_id The operation id.
        # @param [ Hash ] command_payload The command message payload.
        # @param [ Hash ] reply_payload The reply message payload.
        # @param [ Float ] duration The duration of the command in seconds.
        # @param [ Monitoring::Event::CommandStarted ] started_event The corresponding
        #   started event.
        # @param [ Object ] service_id The service id, if any.
        #
        # @return [ CommandCompleted ] The event.
        #
        # @since 2.1.0
        # @api private
        def self.generate(address, operation_id, command_payload,
          reply_payload, duration, started_event:, server_connection_id: nil,
          service_id: nil
        )
          new(
            command_payload[:command_name],
            command_payload[:database_name],
            address,
            command_payload[:request_id],
            operation_id,
            generate_reply(command_payload, reply_payload),
            duration,
            started_event: started_event,
            server_connection_id: server_connection_id,
            service_id: service_id,
          )
        end

        private

        def self.generate_reply(command_payload, reply_payload)
          if reply_payload
            reply = reply_payload[:reply]
            if cursor = reply[:cursor]
              if !cursor.key?(Collection::NS)
                cursor.merge!(Collection::NS => namespace(command_payload))
              end
            end
            reply
          else
            BSON::Document.new(Operation::Result::OK => 1)
          end
        end

        def self.namespace(payload)
          command = payload[:command]
          "#{payload[:database_name]}.#{command[:collection] || command.values.first}"
        end
      end
    end
  end
end
