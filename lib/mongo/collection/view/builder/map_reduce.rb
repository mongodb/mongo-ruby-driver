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
      module Builder

        # Builds a map/reduce specification from the view and options.
        #
        # @since 2.2.0
        class MapReduce
          extend Forwardable

          # The mappings from ruby options to the map/reduce options.
          #
          # @since 2.2.0
          MAPPINGS = BSON::Document.new(
            finalize: 'finalize',
            js_mode: 'jsMode',
            out: 'out',
            scope: 'scope',
            verbose: 'verbose',
            bypass_document_validation: 'bypassDocumentValidation'
          ).freeze

          def_delegators :@view, :collection, :database, :filter, :read

          # @return [ String ] map The map function.
          attr_reader :map

          # @return [ String ] reduce The reduce function.
          attr_reader :reduce

          # @return [ Collection::View ] view The collection view.
          attr_reader :view

          # @return [ Hash ] options The map/reduce specific options.
          attr_reader :options

          # Initialize the builder.
          #
          # @example Initialize the builder.
          #   MapReduce.new(map, reduce, view, options)
          #
          # @param [ String ] map The map function.
          # @param [ String ] reduce The reduce function.
          # @param [ Collection::View ] view The collection view.
          # @param [ Hash ] options The map/reduce options.
          #
          # @since 2.2.0
          def initialize(map, reduce, view, options)
            @map = map
            @reduce = reduce
            @view = view
            @options = options
          end

          # Get the specification for issuing a find command on the map/reduce
          # results.
          #
          # @example Get the command specification.
          #   builder.command_specification
          #
          # @return [ Hash ] The specification.
          #
          # @since 2.2.0
          def command_specification
            { selector: find_command, db_name: database.name, read: read }
          end

          # Get the specification for the document query after a map/reduce.
          #
          # @example Get the query specification.
          #   builder.query_specification
          #
          # @return [ Hash ] The specification.
          #
          # @since 2.2.0
          def query_specification
            { selector: {}, options: {}, db_name: database.name, coll_name: query_collection }
          end

          # Get the specification to pass to the map/reduce operation.
          #
          # @example Get the specification.
          #   builder.specification
          #
          # @return [ Hash ] The specification.
          #
          # @since 2.2.0
          def specification
            { selector: map_reduce_command, db_name: database.name, read: read }
          end

          private

          def find_command
            BSON::Document.new('find' => query_collection, 'filter' => {})
          end

          def map_reduce_command
            command = BSON::Document.new(
              :mapreduce => collection.name,
              :map => map,
              :reduce => reduce,
              :query => filter,
              :out => { inline: 1 }
            )
            command[:readConcern] = collection.read_concern if collection.read_concern
            command.merge!(Options::Mapper.transform_documents(options, MAPPINGS))
            command.merge!(view.options)
            command
          end

          def query_collection
            options[:out].respond_to?(:keys) ? options[:out].values.first : options[:out]
          end
        end
      end
    end
  end
end
