# Copyright (C) 2015 MongoDB, Inc.
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

        # @return [ BSON::Document ] command The command arguments.
        attr_reader :command

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

        # Create the new event.
        #
        # @example Create the event.
        #
        # @param [ String ] command_name The name of the command.
        # @param [ BSON::Document ] command The command arguments.
        # @param [ String ] database_name The database name.
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] request_id The request id.
        # @param [ Integer ] operation_id The operation id.
        # @param [ BSON::Document ] reply The command reply.
        # @param [ Float ] duration The duration the command took in seconds.
        #
        # @since 2.1.0
        def initialize(command_name, command, database_name, address, request_id, operation_id, reply, duration)
          @command_name = command_name
          @command = command
          @database_name = database_name
          @address = address
          @request_id = request_id
          @operation_id = operation_id
          @reply = redacted(command_name, reply)
          @duration = duration
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
        #
        # @return [ CommandCompleted ] The event.
        #
        # @since 2.1.0
        def self.generate(address, operation_id, command_payload, reply_payload, duration)
          new(
            command_payload[:command_name],
            command_payload[:command],
            command_payload[:database_name],
            address,
            command_payload[:request_id],
            operation_id,
            generate_reply(command_payload, reply_payload),
            duration
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
