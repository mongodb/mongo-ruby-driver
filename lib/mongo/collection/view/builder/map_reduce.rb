# Copyright (C) 2015-2017 MongoDB, Inc.
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
            bypass_document_validation: 'bypassDocumentValidation',
            collation: 'collation'
          ).freeze

          def_delegators :@view, :collection, :database, :filter, :read, :write_concern

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
            {
              selector: find_command,
              db_name: query_database,
              read: read
            }
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
            { selector: {}, options: {}, db_name: query_database, coll_name: query_collection }
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
            spec = {
              selector: map_reduce_command,
              db_name: database.name,
              read: read
            }
            write?(spec) ? spec.merge!(write_concern: write_concern) : spec
          end

          private

          OUT_ACTIONS = [ :replace, :merge, :reduce ].freeze

          def write?(spec)
            if out = spec[:selector][:out]
              out.is_a?(String) ||
                (out.respond_to?(:keys) && out.keys.first.to_s.downcase != View::MapReduce::INLINE)
            end
          end

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
            command.merge!(view.options)
            command.merge!(Options::Mapper.transform_documents(options, MAPPINGS))
            command
          end

          def query_database
            options[:out].respond_to?(:keys) && options[:out][:db] ? options[:out][:db] : database.name
          end

          def query_collection
            if options[:out].respond_to?(:keys)
              options[:out][OUT_ACTIONS.find { |action| options[:out][action] }]
            end || options[:out]
          end
        end
      end
    end
  end
end
