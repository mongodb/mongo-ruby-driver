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
      module Bulk

        # Provides common behavior for bulk write operations.
        # Note that #validate! is not called on operation results because they are merged
        # at a higher level.
        #
        # @since 2.1.0
        module Bulkable

          # Execute the bulk operation.
          #
          # @example Execute the operation.
          #   operation.execute(server)
          #
          # @param [ Mongo::Server ] server The server to send this operation to.
          #
          # @return [ Result ] The operation result.
          #
          # @since 2.0.0
          def execute(server)
            if server.features.write_command_enabled?
              execute_write_command(server)
            else
              execute_message(server)
            end
          end

          private

          def execute_message(server)
            replies = messages.map do |m|
              server.with_connection do |connection|
                result = self.class::LegacyResult.new(connection.dispatch([ m, gle ].compact, operation_id))
                if stop_sending?(result)
                  return result
                else
                  result.reply
                end
              end
            end
            self.class::LegacyResult.new(replies.compact.empty? ? nil : replies)
          end

          def stop_sending?(result)
            ordered? && !result.successful?
          end

          def gle
            wc = write_concern ||  WriteConcern.get(WriteConcern::DEFAULT)
            gle_message = ( ordered? && wc.get_last_error.nil? ) ?
                WriteConcern.get(WriteConcern::DEFAULT).get_last_error :
                wc.get_last_error
            if gle_message
              Protocol::Query.new(
                  db_name,
                  Database::COMMAND,
                  gle_message,
                  options.merge(limit: -1)
              )
            end
          end
        end
      end
    end
  end
end
