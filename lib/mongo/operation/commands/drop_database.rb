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
    module Commands

      # A MongoDB drop database operation.
      #
      # @example Instantiate the operation.
      #   Commands::Drop.new(selector: { dropDatabase: 'test' }, :db_name => 'test')
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the operation.
      #
      #   option spec :db_name [ String ] The name of the database.
      #   option spec :selector [ Hash ] The drop database selector.
      #   option spec :write_concern [ String ] The write concern to use.
      #     Only applied for server version >= 3.4.
      #
      # @since 2.4.0
      class DropDatabase < Command
        include TakesWriteConcern

        private

        def message(server)
          sel = update_selector_for_write_concern(selector, server)
          Protocol::Query.new(db_name, query_coll, sel, options)
        end
      end
    end
  end
end
