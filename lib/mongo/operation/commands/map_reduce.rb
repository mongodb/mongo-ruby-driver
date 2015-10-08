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
    module Commands

      # A MongoDB map reduce operation.
      #
      # @note A map/reduce operation can behave like a read and
      #   return a result set, or can behave like a write operation and
      #   output results to a user-specified collection.
      #
      # @example Create the map/reduce operation.
      #   MapReduce.new({
      #     :selector => {
      #       :mapreduce => 'test_coll',
      #       :map => '',
      #       :reduce => ''
      #     },
      #     :db_name  => 'test_db'
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the operation.
      #
      #   option spec :selector [ Hash ] The map reduce selector.
      #   option spec :db_name [ String ] The name of the database on which
      #     the operation should be executed.
      #   option spec :options [ Hash ] Options for the map reduce command.
      #
      # @since 2.0.0
      class MapReduce < Command; end
    end
  end
end

require 'mongo/operation/commands/map_reduce/result'
