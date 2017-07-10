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
    module Read

      # A MongoDB get more operation.
      #
      # @example Create a get more operation.
      #   Read::GetMore.new({
      #     :to_return => 50,
      #     :cursor_id => 1,
      #     :db_name   => 'test_db',
      #     :coll_name => 'test_coll'
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the operation.
      #
      #   option spec :to_return [ Integer ] The number of results to return.
      #   option spec :cursor_id [ Integer ] The id of the cursor.
      #   option spec :db_name [ String ] The name of the database on which
      #     the operation should be executed.
      #   option spec :coll_name [ String ] The name of the collection on which
      #     the operation should be executed.
      #
      # @since 2.0.0
      class GetMore
        include Specifiable
        include Executable

        private

        def message(server)
          Protocol::GetMore.new(db_name, coll_name, to_return, cursor_id)
        end
      end
    end
  end
end
