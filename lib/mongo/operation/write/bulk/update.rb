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

require 'mongo/operation/write/bulk/update/result'

module Mongo
  module Operation
    module Write
      module Bulk

        # A MongoDB bulk update operation.
        #
        # @note If the server version is >= 2.5.5, a write command operation
        #   will be created and sent instead.
        #
        # @example Create the update operation.
        #   Write::BulkUpdate.new({
        #     :updates => [
        #       {
        #         :q => { :foo => 1 },
        #         :u => { :$set => { :bar => 1 }},
        #         :multi  => true,
        #         :upsert => false
        #       }
        #     ],
        #     :db_name => 'test',
        #     :coll_name => 'test_coll',
        #     :write_concern => write_concern,
        #     :ordered => false
        #   })
        #
        # Initialization:
        #   param [ Hash ] spec The specifications for the update.
        #
        #   option spec :updates [ Array ] The update documents.
        #   option spec :db_name [ String ] The name of the database on which
        #     the query should be run.
        #   option spec :coll_name [ String ] The name of the collection on which
        #     the query should be run.
        #   option spec :write_concern [ Mongo::WriteConcern ] The write concern.
        #   option spec :ordered [ true, false ] Whether the operations should be
        #     executed in order.
        #   option spec :options [ Hash ] Options for the command, if it ends up being a
        #     write command.
        #
        # @since 2.0.0
        class Update
          include Bulkable
          include Specifiable

          private

          def execute_write_command(server)
            Result.new(Command::Update.new(spec).execute(server))
          end


          def messages
            updates.collect do |u|
              opts = { :flags => [] }
              opts[:flags] << :multi_update if !!u[Operation::MULTI]
              opts[:flags] << :upsert if !!u[Operation::UPSERT]
              Protocol::Update.new(db_name, coll_name, u[Operation::Q], u[Operation::U], opts)
            end
          end
        end
      end
    end
  end
end
