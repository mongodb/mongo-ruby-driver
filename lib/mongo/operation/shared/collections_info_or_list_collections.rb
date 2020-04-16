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
  module Operation
    module CollectionsInfoOrListCollections
      include PolymorphicLookup

      # Execute the operation.
      #
      # @example
      #   operation.execute(server, client: nil)
      #
      # @param [ Mongo::Server ] server The server to send the operation to.
      # @param [ Mongo::Client ] client The client that will be used to
      #   perform auto-encryption if it is necessary to encrypt the command
      #   being executed (optional).
      #
      # @return [ Mongo::Operation::CollectionsInfo::Result,
      #           Mongo::Operation::ListCollections::Result ] The operation result.
      #
      # @since 2.0.0
      def execute(server, client:)
        server.with_connection do |connection|
          operation = final_operation(connection)
          operation.execute(connection, client: client)
        end
      end

      private

      def final_operation(connection)
         op_class = if connection.features.list_collections_enabled?
          if connection.features.op_msg_enabled?
            ListCollections::OpMsg
          else
            ListCollections::Command
          end
        else
          CollectionsInfo::Command
        end

        op_class.new(spec)
      end
    end
  end
end
