# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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

    # Shared behavior of operations that write (update, insert, delete).
    #
    # @since 2.5.2
    # @api private
    module Write

      include ResponseHandling

      # Execute the operation.
      #
      # @param [ Mongo::Server ] server The server to send the operation to.
      # @param [ Operation::Context ] context The operation context.
      #
      # @return [ Mongo::Operation::Result ] The operation result.
      #
      # @since 2.5.2
      def execute(server, context:)
        server.with_connection(connection_global_id: context.connection_global_id) do |connection|
          execute_with_connection(connection, context: context)
        end
      end

      # Execute the operation.
      #
      # @param [ Mongo::Server::Connection ] connection The connection to send
      #   the operation through.
      # @param [ Operation::Context ] context The operation context.
      # @param [ Hash ] options Operation execution options.
      #
      # @return [ Mongo::Operation::Result ] The operation result.
      def execute_with_connection(connection, context:)
        validate!(connection)
        op = self.class::OpMsg.new(spec)

        result = op.execute(connection, context: context)
        validate_result(result, connection, context)
      end

      # Execute the bulk write operation.
      #
      # @param [ Mongo::Server::Connection ] connection The connection over
      #   which to send the operation.
      # @param [ Operation::Context ] context The operation context.
      #
      # @return [ Mongo::Operation::Delete::BulkResult,
      #           Mongo::Operation::Insert::BulkResult,
      #           Mongo::Operation::Update::BulkResult ] The bulk result.
      #
      # @since 2.5.2
      def bulk_execute(connection, context:)
        Lint.assert_type(connection, Server::Connection)

        if connection.features.op_msg_enabled?
          self.class::OpMsg.new(spec).execute(connection, context: context).bulk_result
        else
          self.class::Command.new(spec).execute(connection, context: context).bulk_result
        end
      end

      private

      def validate!(connection)
        if !acknowledged_write?
          if collation
            raise Error::UnsupportedCollation.new(
                Error::UnsupportedCollation::UNACKNOWLEDGED_WRITES_MESSAGE)
          end
          if array_filters(connection)
            raise Error::UnsupportedArrayFilters.new(
                Error::UnsupportedArrayFilters::UNACKNOWLEDGED_WRITES_MESSAGE)
          end
        end
      end
    end
  end
end
