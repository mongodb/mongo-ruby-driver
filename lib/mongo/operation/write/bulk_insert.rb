# Copyright (C) 2009-2014 MongoDB, Inc.
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
      # @param [ Hash ] spec The specifications for the insert.
      #
      # @option spec :documents [ Array ] The documents to insert.
      # @option spec :db_name [ String ] The name of the database.
      # @option spec :coll_name [ String ] The name of the collection.
      # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
      # @option spec :ordered [ true, false ] Whether the operations should be
      #   executed in order.
      # @option spec :options [ Hash ] Options for the command, if it ends up being a
      #   write command.
      #
      # @since 2.0.0
      class BulkInsert
        include Executable
        include Specifiable

        # Execute the bulk insert operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Result ] The operation result.
        #
        # @since 2.0.0
        def execute(context)
          if context.write_command_enabled?
            execute_write_command(context)
          else
            execute_message(context)
          end
        end

        private

        def execute_write_command(context)
          Result.new(Command::Insert.new(spec).execute(context)).validate!
        end

        def execute_message(context)
          replies = messages(context).map do |m|
            context.with_connection do |connection|
              # @todo: only validate if it's ordered
              Result.new(connection.dispatch([ m, gle ])).validate!.reply
            end
          end
          Result.new(replies)
        end

        def ordered?
          @spec.fetch(:ordered, true)
        end

        def gle
          gle_message = ordered? ? Mongo::WriteConcern::Mode.get(:w => 1).get_last_error :
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

        def initialize_copy(original)
          @spec = original.spec.dup
          @spec[:documents] = original.spec[:documents].dup
        end

        def messages(context)
          # @todo: break up into multiple messages depending on max_message_size
          if ordered?
            documents.collect do |doc|
              Protocol::Insert.new(db_name, coll_name, [ doc ], options)
            end
          else
            [ Protocol::Insert.new(db_name, coll_name, documents, { :flags => [:continue_on_error] }) ]
            # @todo: check size after serialization and split if it's too large.
          end
        end
      end
    end
  end
end
