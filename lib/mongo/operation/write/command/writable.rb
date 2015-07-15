# Copyright (C) 2014-2015 MongoDB, Inc.
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
    module Write
      module Command

        # Provides common behavior for write commands.
        # Assigns an operation id when executed.
        #
        # @since 2.0.0
        module Writable
          include Limited

          # Execute the operation.
          # The context gets a connection on which the operation
          # is sent in the block.
          #
          # @param [ Mongo::Server::Context ] context The context for this operation.
          #
          # @return [ Result ] The operation response, if there is one.
          #
          # @since 2.0.0
          def execute(context)
            context.with_connection do |connection|
              connection.dispatch([ message ], operation_id)
            end
          end

          private

          # The wire protocol message for this write operation.
          #
          # @return [ Mongo::Protocol::Query ] Wire protocol message.
          #
          # @since 2.0.0
          def message
            Protocol::Query.new(db_name, Database::COMMAND, selector, options)
          end
        end
      end
    end
  end
end
