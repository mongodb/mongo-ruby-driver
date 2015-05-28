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

      # Event that is fired when a command operation starts.
      #
      # @since 2.1.0
      class CommandStarted

        # @return [ Server::Address ] address The server address.
        attr_reader :address

        # @return [ BSON::Document ] command_args The command arguments.
        attr_reader :command_args

        # @return [ String ] command_name The name of the command.
        attr_reader :command_name

        # @return [ String ] database The name of the database.
        attr_reader :database

        # @return [ Array<BSON::Document ] input_docs The input documents.
        attr_reader :input_docs

        # @return [ BSON::Document ] metadata The command metadata.
        attr_reader :metadata

        # @return [ Integer ] operation_id The operation id.
        attr_reader :operation_id

        # @return [ Integer ] request_id The request id.
        attr_reader :request_id

        # Create the new event.
        #
        # @example Create the event.
        #
        # @param [ String ] command_name The name of the command.
        # @param [ String ] database The database name.
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] request_id The request id.
        # @param [ Integer ] operation_id The operation id.
        # @param [ BSON::Document ] command_args The command arguments.
        # @param [ BSON::Document ] metadata The command metadata.
        # @param [ Array<BSON::Document> ] input_docs The input documents.
        #
        # @since 2.1.0
        def initialize(command_name, database, address, request_id, operation_id, command_args, metadata, input_docs)
          @command_name = command_name
          @database = database
          @address = address
          @request_id = request_id
          @operation_id = operation_id
          @command_args = command_args
          @metadata = metadata
          @input_docs = input_docs
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
        def self.generate(address, operation_id, payload)
          new(
            payload[:command_name],
            payload[:database],
            address,
            payload[:request_id],
            operation_id,
            payload[:command_args],
            payload[:metadata],
            payload[:input_docs]
          )
        end
      end
    end
  end
end
