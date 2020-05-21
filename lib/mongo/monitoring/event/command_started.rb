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

      # Event that is fired when a command operation starts.
      #
      # @since 2.1.0
      class CommandStarted < Mongo::Event::Base
        include Secure

        # @return [ Server::Address ] address The server address.
        attr_reader :address

        # @return [ BSON::Document ] command The command arguments.
        attr_reader :command

        # @return [ String ] command_name The name of the command.
        attr_reader :command_name

        # @return [ String ] database_name The name of the database_name.
        attr_reader :database_name

        # @return [ Integer ] operation_id The operation id.
        attr_reader :operation_id

        # @return [ Integer ] request_id The request id.
        attr_reader :request_id

        # object_id of the socket object used for this command.
        #
        # @api private
        attr_reader :socket_object_id

        # @api private
        attr_reader :connection_generation

        # @return [ Integer ] The ID for the connection over which the command
        #   is sent.
        #
        # @api private
        attr_reader :connection_id

        # Create the new event.
        #
        # @example Create the event.
        #
        # @param [ String ] command_name The name of the command.
        # @param [ String ] database_name The database_name name.
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] request_id The request id.
        # @param [ Integer ] operation_id The operation id.
        # @param [ BSON::Document ] command The command arguments.
        #
        # @since 2.1.0
        # @api private
        def initialize(command_name, database_name, address, request_id,
          operation_id, command, socket_object_id: nil, connection_id: nil,
          connection_generation: nil
        )
          @command_name = command_name.to_s
          @database_name = database_name
          @address = address
          @request_id = request_id
          @operation_id = operation_id
          @command = redacted(command_name, command)
          @socket_object_id = socket_object_id
          @connection_id = connection_id
          @connection_generation = connection_generation
        end

        # Create the event from a wire protocol message payload.
        #
        # @example Create the event.
        #   CommandStarted.generate(address, 1, payload)
        #
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] operation_id The operation id.
        # @param [ Hash ] payload The message payload.
        #
        # @return [ CommandStarted ] The event.
        #
        # @since 2.1.0
        # @api private
        def self.generate(address, operation_id, payload,
          socket_object_id: nil, connection_id: nil, connection_generation: nil
        )
          new(
            payload[:command_name],
            payload[:database_name],
            address,
            payload[:request_id],
            operation_id,
            # All op_msg  payloads have a $db field. Legacy payloads do not
            # have a $db field. To emulate op_msg when publishing command
            # monitoring events for legacy servers, add $db to the payload,
            # copying the database name. Note that the database name is also
            # available as a top-level attribute on the command started event.
            payload[:command].merge('$db' => payload[:database_name]),
            socket_object_id: socket_object_id,
            connection_id: connection_id,
            connection_generation: connection_generation,
          )
        end

        # Returns a concise yet useful summary of the event.
        #
        # @return [ String ] String summary of the event.
        #
        # @since 2.6.0
        def inspect
          "#<{#{self.class} #{database_name}.#{command_name} command=#{command}>"
        end
      end
    end
  end
end
