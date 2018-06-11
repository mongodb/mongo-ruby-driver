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

      # Event that is fired when a command operation fails.
      #
      # @since 2.1.0
      class CommandFailed < Mongo::Event::Base

        # @return [ Server::Address ] address The server address.
        attr_reader :address

        # @return [ String ] command_name The name of the command.
        attr_reader :command_name

        # @return [ String ] database_name The name of the database_name.
        attr_reader :database_name

        # @return [ Float ] duration The duration of the command in seconds.
        attr_reader :duration

        # @return [ String ] message The error message.
        attr_reader :message

        # @return [ Integer ] operation_id The operation id.
        attr_reader :operation_id

        # @return [ Integer ] request_id The request id.
        attr_reader :request_id

        # Create the new event.
        #
        # @example Create the event.
        #
        # @param [ String ] command_name The name of the command.
        # @param [ String ] database_name The database_name name.
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] request_id The request id.
        # @param [ Integer ] operation_id The operation id.
        # @param [ String ] message The error message.
        # @param [ Float ] duration The duration the command took in seconds.
        #
        # @since 2.1.0
        def initialize(command_name, database_name, address, request_id, operation_id, message, duration)
          @command_name = command_name
          @database_name = database_name
          @address = address
          @request_id = request_id
          @operation_id = operation_id
          @message = message
          @duration = duration
        end

        # Create the event from a wire protocol message payload.
        #
        # @example Create the event.
        #   CommandFailed.generate(address, 1, payload, duration)
        #
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] operation_id The operation id.
        # @param [ Hash ] payload The message payload.
        # @param [ String ] message The error message.
        # @param [ Float ] duration The duration of the command in seconds.
        #
        # @return [ CommandFailed ] The event.
        #
        # @since 2.1.0
        def self.generate(address, operation_id, payload, message, duration)
          new(
            payload[:command_name],
            payload[:database_name],
            address,
            payload[:request_id],
            operation_id,
            message,
            duration
          )
        end
      end
    end
  end
end
