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

      # Builds a find command specification from options.
      #
      # @since 2.2.0
      class FindCommandBuilder

        # The mappings from ruby options to the find command.
        #
        # @since 2.2.0
        MAPPINGS = BSON::Document.new(
          sort: 'sort',
          projection: 'projection',
          hint: 'hint',
          skip: 'skip',
          limit: 'limit',
          batch_size: 'batchSize',
          single_batch: 'singleBatch',
          comment: 'comment',
          max_scan: 'maxScan',
          max_time_ms: 'maxTimeMS',
          max_value: 'max',
          min_value: 'min',
          return_key: 'returnKey',
          show_disk_loc: 'showRecordId',
          snapshot: 'snapshot',
          tailable: 'tailable',
          oplog_replay: 'oplogReplay',
          no_cursor_timeout: 'noCursorTimeout',
          await_data: 'awaitData',
          allow_partial_results: 'allowPartialResults'
        ).freeze

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

        # Get the specification to pass to the find command operation.
        #
        # @example Get the specification.
        #   builder.specification
        #
        # @return [ Hash ] The specification.
        #
        # @since 2.2.0
        def specification
          { selector: find_command, db_name: database.name, read: options[:read] }
        end

        private

        def find_command
          options.reduce({ 'find' => collection.name, 'filter' => filter }) do |command, (key, value)|
            name = MAPPINGS[key]
            command[name] = value if name && !value.nil?
            command
          end
        end
      end
    end
  end
end
