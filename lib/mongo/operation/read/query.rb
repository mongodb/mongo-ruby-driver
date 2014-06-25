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

      # A MongoDB query operation.
      #
      # @since 3.0.0
      class Query
        include Executable

        # Initialize the query operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Read::Query.new(collection,
        #                   :selector => { :foo => 1 },
        #                   :opts => { :limit => 2 })
        #
        # @param [ Collection ] collection The collection on which the query
        #   should be run.
        # @param [ Hash ] spec The specifications for the query.
        #
        # @option spec :selector [ Hash ] The query selector.
        # @option spec :opts [ Hash ] Options for the query.
        #
        # @since 3.0.0
        def initialize(collection, spec)
          @collection = collection
          @spec       = spec
        end

        private

        # The selector for the query.
        #
        # @return [ Hash ] The query selector. 
        #
        # @since 3.0.0
        def selector
          @spec[:selector]
        end

        # The options for the query.
        #
        # @return [ Hash ] The query options.
        #
        # @since 3.0.0
        def opts
          @spec[:opts] || {}
        end

        # The wire protocol message for this query operation.
        #
        # @return [ Mongo::Protocol::Query ] Wire protocol message.
        #
        # @since 3.0.0
        def message
          Mongo::Protocol::Query.new(db_name, coll_name, selector, opts)
        end
      end
    end
  end
end
