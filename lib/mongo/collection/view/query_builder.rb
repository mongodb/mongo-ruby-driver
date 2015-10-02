# Copyright (C) 2015 MongoDB, Inc.
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
  class Collection
    class View

      # Builds a legacy OP_QUERY specification from options.
      #
      # @since 2.2.0
      class QueryBuilder

        OPTION_MAPPINGS = BSON::Document.new(

        ).freeze

        MODIFIER_MAPPINGS = BSON::Document.new(

        ).freeze

        FLAG_FIELDS = [

        ].freeze

        # @return [ Collection ] collection The collection.
        attr_reader :collection

        # @return [ Database ] database The database.
        attr_reader :database

        # @return [ Hash, BSON::Documnet ] filter The filter.
        attr_reader :filter

        # @return [ Hash, BSON::Document ] options The options.
        attr_reader :options

        # Create the new legacy query builder.
        #
        # @example Create the query builder.
        #   QueryBuilder.new(collection, database, {}, {})
        #
        # @param [ Collection ] collection The collection.
        # @param [ Database ] database The database.
        # @param [ Hash, BSON::Document ] filter The filter.
        # @param [ Hash, BSON::Document ] options The options.
        #
        # @since 2.2.2
        def initialize(collection, database, filter, options)
          @collection = collection
          @database = database
          @filter = filter
          @options = options
        end

        def specification
          {
            :selector  => filter,
            :read      => read,
            :options   => options,
            :db_name   => database.name,
            :coll_name => collection.name
          }
        end
      end
    end
  end
end
