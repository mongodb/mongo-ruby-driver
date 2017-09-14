# Copyright (C) 2014-2017 MongoDB, Inc.
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
          include UsesCommandOpMsg

          # Execute the operation.
          #
          # @example Execute the operation.
          #   operation.execute(server)
          #
          # @param [ Mongo::Server ] server The server to send this operation to.
          #
          # @return [ Result ] The operation response, if there is one.
          #
          # @since 2.0.0
          def execute(server)
            server.with_connection do |connection|
              connection.dispatch([ message(server) ], operation_id)
            end
          end

          private

          def command_options
            opts = { ordered: ordered? }
            opts[:writeConcern] = write_concern.options if write_concern
            opts[:collation] = collation if collation
            opts[:bypassDocumentValidation] = true if bypass_document_validation
            opts
          end

          # The wire protocol message for this write operation.
          #
          # @return [ Mongo::Protocol::Query ] Wire protocol message.
          #
          # @since 2.0.0
          def message(server)
            if server.features.op_msg_enabled?
              command_op_msg(server, selector, options)
            else
              Protocol::Query.new(db_name, Database::COMMAND, selector, options)
            end
          end
        end
      end
    end
  end
end
