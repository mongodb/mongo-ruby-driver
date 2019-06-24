# Copyright (C) 2018-2019 MongoDB, Inc.
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
    module Write

      # Execute the operation.
      #
      # @example
      #   operation.execute(server)
      #
      # @param [ Mongo::Server ] server The server to send the operation to.
      #
      # @return [ Mongo::Operation::Result ] The operation result.
      #
      # @since 2.5.2
      def execute(server)
        validate!
        result = if server.features.op_msg_enabled?
            self.class::OpMsg.new(spec).execute(server)
          elsif !acknowledged_write?
            self.class::Legacy.new(spec).execute(server)
          else
            self.class::Command.new(spec).execute(server)
          end
        validate_result(result)
      end

      # Execute the bulk write operation.
      #
      # @example
      #   operation.bulk_execute(server)
      #
      # @param [ Mongo::Server ] server The server to send the operation to.
      #
      # @return [ Mongo::Operation::Delete::BulkResult,
      #           Mongo::Operation::Insert::BulkResult,
      #           Mongo::Operation::Update::BulkResult ] The bulk result.
      #
      # @since 2.5.2
      def bulk_execute(server)
        if server.features.op_msg_enabled?
          self.class::OpMsg.new(spec).execute(server).bulk_result
        else
          self.class::Command.new(spec).execute(server).bulk_result
        end
      end

      private

      def validate!
        if !acknowledged_write?
          if collation
            raise Error::UnsupportedCollation.new(
                Error::UnsupportedCollation::UNACKNOWLEDGED_WRITES_MESSAGE)
          end
          if array_filters
            raise Error::UnsupportedArrayFilters.new(
                Error::UnsupportedArrayFilters::UNACKNOWLEDGED_WRITES_MESSAGE)
          end
        end
      end
    end
  end
end
