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

require 'mongo/operation/write/bulk/insert/result'

module Mongo
  module Operation
    module Write
      module Bulk

        # A MongoDB bulk insert operation.
        # This class should only be used by the Bulk API.
        #
        # @note If a server with version >= 2.5.5 is being used, a write command
        #   operation will be created and sent instead.
        #
        # @example Create the new insert operation.
        #   Write::BulkInsert.new({
        #     :documents => [{ :foo => 1 }],
        #     :db_name => 'test',
        #     :coll_name => 'test_coll',
        #     :write_concern => write_concern,
        #     :ordered => false
        #   })
        #
        # Initialization:
        #   param [ Hash ] spec The specifications for the insert.
        #
        #   option spec :documents [ Array ] The documents to insert.
        #   option spec :db_name [ String ] The name of the database.
        #   option spec :coll_name [ String ] The name of the collection.
        #   option spec :write_concern [ Mongo::WriteConcern ] The write concern.
        #   option spec :ordered [ true, false ] Whether the operations should be
        #     executed in order.
        #   option spec :options [ Hash ] Options for the command, if it ends up being a
        #     write command.
        #
        # @since 2.0.0
        class Insert
          include Bulkable
          include Specifiable
          include Idable

          private

          def execute_write_command(server)
            command_spec = spec.merge(:documents => ensure_ids(documents))
            Result.new(Command::Insert.new(command_spec).execute(server), @ids)
          end

          def execute_message(server)
            replies = []
            messages.map do |m|
              server.with_connection do |connection|
                result = LegacyResult.new(connection.dispatch([ m, gle ].compact, operation_id), @ids)
                replies << result.reply
                if stop_sending?(result)
                  return LegacyResult.new(replies, @ids)
                end
              end
            end
            LegacyResult.new(replies.compact.empty? ? nil : replies, @ids)
          end

          def messages
            if ordered? || gle
              documents.collect do |doc|
                Protocol::Insert.new(db_name, coll_name, ensure_ids([ doc ]), spec)
              end
            else
              [
                Protocol::Insert.new(
                  db_name,
                  coll_name,
                  ensure_ids(documents),
                  spec.merge({ :flags => [:continue_on_error] })
                )
              ]
            end
          end
        end
      end
    end
  end
end
