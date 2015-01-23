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

module Mongo
  module Operation
    module Write

      # A MongoDB drop index operation.
      #
      # @example Create the drop index operation.
      #   Write::DropIndex.new({
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :index_name => 'name_1_age_-1'
      #   })
      #
      # @param [ Hash ] spec The specifications for the drop.
      #
      # @option spec :index [ Hash ] The index spec to create.
      # @option spec :db_name [ String ] The name of the database.
      # @option spec :coll_name [ String ] The name of the collection.
      # @option spec :index_name [ String ] The name of the index.
      #
      # @since 2.0.0
      class DropIndex
        include Specifiable

        # Execute the drop index operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Result ] The result of the operation.
        #
        # @since 2.0.0
        def execute(context)
          execute_write_command(context)
        end

        private

        def execute_write_command(context)
          Result.new(Command::DropIndex.new(spec).execute(context)).validate!
        end
      end
    end
  end
end
