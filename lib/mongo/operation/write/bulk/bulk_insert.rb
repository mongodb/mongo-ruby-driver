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

require 'mongo/operation/write/bulk/bulk_insert/result'

module Mongo
  module Operation
    module Write

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
      class BulkInsert
        include Specifiable
        include Idable

        # Execute the bulk insert operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @param [ Mongo::Server::Context ] context The context for this operation.
        #
        # @return [ Result ] The operation result.
        #
        # @since 2.0.0
        def execute(context)
          if context.features.write_command_enabled?
            execute_write_command(context)
          else
            execute_message(context)
          end
        end

        private

        def execute_write_command(context)
          command_spec = spec.merge(:documents => ensure_ids(documents))
          Result.new(Command::Insert.new(command_spec).execute(context), @ids)
        end

        def execute_message(context)
          replies = []
          messages.map do |m|
            context.with_connection do |connection|
              result = LegacyResult.new(connection.dispatch([ m, gle ].compact), @ids)
              replies << result.reply
              if stop_sending?(result)
                return LegacyResult.new(replies, @ids)
              end
            end
          end
          LegacyResult.new(replies.compact.empty? ? nil : replies, @ids)
        end

        def stop_sending?(result)
          ordered? && !result.successful?
        end

        # @todo put this somewhere else
        def ordered?
          @spec.fetch(:ordered, true)
        end

        def initialize_copy(original)
          @spec = original.spec.dup
          @spec[DOCUMENTS] = original.spec[DOCUMENTS].clone
        end

        def gle
          gle_message = ( ordered? && write_concern.get_last_error.nil? ) ?
                           Mongo::WriteConcern.get(:w => 1).get_last_error :
                           write_concern.get_last_error
          if gle_message
            Protocol::Query.new(
              db_name,
              Database::COMMAND,
              gle_message,
              options.merge(limit: -1)
            )
          end
        end

        def messages
          if ordered? || gle
            documents.collect do |doc|
              Protocol::Insert.new(db_name, coll_name, ensure_ids([ doc ]), options)
            end
          else
            [ Protocol::Insert.new(db_name, coll_name, ensure_ids(documents), { :flags => [:continue_on_error] }) ]
          end
        end
      end
    end
  end
end
