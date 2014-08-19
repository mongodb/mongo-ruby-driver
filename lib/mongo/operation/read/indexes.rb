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
    module Read

      # A MongoDB get indexes operation.
      #
      # @since 2.0.0
      class Indexes
        include Executable

        # Initialize the get indexes operation.
        #
        # @example Instantiate the operation.
        #   Read::Indexes.new(:db_name => 'test', :coll_name => 'test_coll')
        #
        # @param [ Hash ] spec The specifications for the insert.
        #
        # @option spec :db_name [ String ] The name of the database.
        # @option spec :coll_name [ String ] The name of the collection.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
        end

        private

        def message
          Protocol::Query.new(db_name, Indexable::SYSTEM_INDEXES, { ns: namespace }, options)
        end
      end
    end
  end
end
