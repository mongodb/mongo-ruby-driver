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

require 'mongo/operation/read/query/result'

module Mongo
  module Operation
    module Read

      # A MongoDB query operation.
      #
      # @example Create the query operation.
      #   Read::Query.new({
      #     :selector => { :foo => 1 },
      #     :db_name => 'test-db',
      #     :coll_name => 'test-coll',
      #     :options => { :limit => 2 }
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the query.
      #
      #   option spec :selector [ Hash ] The query selector.
      #   option spec :db_name [ String ] The name of the database on which
      #     the query should be run.
      #   option spec :coll_name [ String ] The name of the collection on which
      #     the query should be run.
      #   option spec :options [ Hash ] Options for the query.
      #
      # @since 2.0.0
      class Query
        include Specifiable
        include Executable
        include ReadPreference

        private

        def query_coll
          coll_name
        end
      end
    end
  end
end
