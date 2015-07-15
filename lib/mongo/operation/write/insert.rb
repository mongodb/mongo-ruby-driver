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

require 'mongo/operation/write/insert/result'

module Mongo
  module Operation
    module Write

      # A MongoDB insert operation.
      #
      # @note If a server with version >= 2.5.5 is being used, a write command
      #   operation will be created and sent instead.
      #
      # @example Create the new insert operation.
      #   Write::Insert.new({
      #     :documents => [{ :foo => 1 }],
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :write_concern => write_concern
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the insert.
      #
      #   option spec :documents [ Array ] The documents to insert.
      #   option spec :db_name [ String ] The name of the database.
      #   option spec :coll_name [ String ] The name of the collection.
      #   option spec :write_concern [ Mongo::WriteConcern ] The write concern.
      #   option spec :options [ Hash ] Options for the command, if it ends up being a
      #     write command.
      #
      # @since 2.0.0
      class Insert
        include GLE
        include WriteCommandEnabled
        include Specifiable
        include Idable

        private

        def execute_write_command(context)
          command_spec = spec.merge(:documents => ensure_ids(documents))
          Result.new(Command::Insert.new(command_spec).execute(context), @ids).validate!
        end

        def execute_message(context)
          context.with_connection do |connection|
            Result.new(connection.dispatch([ message, gle ].compact), @ids).validate!
          end
        end

        def message
          opts = !!options[:continue_on_error] ? { :flags => [:continue_on_error] } : {}
          Protocol::Insert.new(db_name, coll_name, ensure_ids(documents), opts)
        end
      end
    end
  end
end
