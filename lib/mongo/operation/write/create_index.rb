
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

      # A MongoDB create index operation.
      #
      # @note If a server with version >= 2.5.5 is being used, a write command
      #   operation will be created and sent instead.
      #
      # @example Create the create index operation.
      #   Write::CreateIndex.new({
      #     :index => { :name => 1, :age => -1 },
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :index_name => 'name_1_age_-1'
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the insert.
      #
      #   option spec :index [ Hash ] The index spec to create.
      #   option spec :db_name [ String ] The name of the database.
      #   option spec :coll_name [ String ] The name of the collection.
      #   option spec :index_name [ String ] The name of the index.
      #   option spec :options [ Hash ] Options for the command, if it ends up being a
      #     write command.
      #
      # @since 2.0.0
      class CreateIndex
        include GLE
        include WriteCommandEnabled
        include Specifiable

        private

        def write_command_op
          Command::CreateIndex.new(spec)
        end

        def index_documents
          indexes.dup.each do |index|
            index[:ns] = namespace
          end
        end

        def message(server)
          Protocol::Insert.new(db_name, Index::COLLECTION, index_documents, validating_keys: false)
        end
      end
    end
  end
end
