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
          include ClusterTime

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

          # The wire protocol message for this write operation.
          #
          # @return [ Mongo::Protocol::Query ] Wire protocol message.
          #
          # @since 2.0.0
          def message(server)
            sel = update_selector_with_cluster_time(selector, server)
            Protocol::Query.new(db_name, Database::COMMAND, sel, options)
          end
        end
      end
    end
  end
end
