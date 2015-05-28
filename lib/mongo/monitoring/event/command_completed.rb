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

      # Event that is fired when a command operation completes.
      #
      # @since 2.1.0
      class CommandCompleted

        # @return [ Server::Address ] address The server address.
        attr_reader :address

        # @return [ String ] command_name The name of the command.
        attr_reader :command_name

        # @return [ BSON::Document ] command_reply The command reply.
        attr_reader :command_reply

        # @return [ String ] database The name of the database.
        attr_reader :database

        # @return [ Float ] duration The duration of the event.
        attr_reader :duration

        # @return [ BSON::Document ] metadata The command metadata.
        attr_reader :metadata

        # @return [ Integer ] operation_id The operation id.
        attr_reader :operation_id

        # @return [ Array<BSON::Document ] output_docs The output documents.
        attr_reader :output_docs

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
        # @param [ BSON::Document ] command_reply The command reply.
        # @param [ BSON::Document ] metadata The command metadata.
        # @param [ Array<BSON::Document> ] output_docs The output documents.
        # @param [ Float ] duration The duration the command took in seconds.
        #
        # @since 2.1.0
        def initialize(command_name, database, address, request_id, operation_id, command_reply, metadata, output_docs, duration)
          @command_name = command_name
          @database = database
          @address = address
          @request_id = request_id
          @operation_id = operation_id
          @command_reply = command_reply
          @metadata = metadata
          @output_docs = output_docs
          @duration = duration
        end

        # Create the event from a wire protocol message payload.
        #
        # @example Create the event.
        #   CommandStarted.generate(address, 1, command_payload, reply_payload, 0.5)
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
            command_payload[:database],
            address,
            command_payload[:request_id],
            operation_id,
            reply_payload ? reply_payload[:command_reply] : nil,
            reply_payload ? reply_payload[:metadata] : nil,
            reply_payload ? reply_payload[:output_docs] : nil,
            duration
          )
        end
      end
    end
  end
end
