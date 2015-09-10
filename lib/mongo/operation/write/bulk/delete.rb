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

require 'mongo/operation/write/bulk/delete/result'

module Mongo
  module Operation
    module Write
      module Bulk

        # A MongoDB bulk delete operation.
        #
        # @note If a server with version >= 2.5.5 is selected, a write command
        #   operation will be created and sent instead.
        #
        # @example Create the delete operation.
        #   Write::Bulk::Delete.new({
        #     :deletes => [{ :q => { :foo => 1 }, :limit => 1 }],
        #     :db_name => 'test',
        #     :coll_name => 'test_coll',
        #     :write_concern => write_concern
        #   })
        #
        # Initialization:
        #   param [ Hash ] spec The specifications for the delete.
        #
        #   option spec :deletes [ Array ] The delete documents.
        #   option spec :db_name [ String ] The name of the database on which
        #     the delete should be executed.
        #   option spec :coll_name [ String ] The name of the collection on which
        #     the delete should be executed.
        #   option spec :write_concern [ Mongo::WriteConcern ] The write concern
        #     for this operation.
        #   option spec :ordered [ true, false ] Whether the operations should be
        #     executed in order.
        #   option spec :options [Hash] Options for the command, if it ends up being a
        #     write command.
        #
        # @since 2.0.0
        class Delete
          include Bulkable
          include Specifiable

          private

          def execute_write_command(context)
            Result.new(Command::Delete.new(spec).execute(context))
          end

          def messages
            deletes.collect do |del|
              opts = ( del[Operation::LIMIT] || 0 ) <= 0 ? {} : { :flags => [ :single_remove ] }
              Protocol::Delete.new(db_name, coll_name, del[Operation::Q], opts)
            end
          end
        end
      end
    end
  end
end
