# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
            collation: 'collation',
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
              # Note that selector just above may also have a read preference
              # specified, per the #map_reduce_command method below.
              read: read,
              session: options[:session]
            }
            write?(spec) ? spec.merge!(write_concern: write_concern) : spec
          end

          private

          def write?(spec)
            if out = spec[:selector][:out]
              out.is_a?(String) ||
                (out.respond_to?(:keys) && out.keys.first.to_s.downcase != View::MapReduce::INLINE)
            end
          end

          def map_reduce_command
            command = BSON::Document.new(
              :mapReduce => collection.name,
              :map => map,
              :reduce => reduce,
              :query => filter,
              :out => { inline: 1 },
            )
            # Shouldn't this use self.read ?
            if collection.read_concern
              command[:readConcern] = Options::Mapper.transform_values_to_strings(
                collection.read_concern)
            end
            command.update(view_options)
            command.update(options.slice(:collation))

            # Read preference isn't simply passed in the command payload
            # (it may need to be converted to wire protocol flags).
            # Ideally it should be removed here, however due to Mongoid 7
            # using this method and requiring :read to be returned from it,
            # we cannot do this just yet - see RUBY-2932.
            #command.delete(:read)

            command.merge!(Options::Mapper.transform_documents(options, MAPPINGS))
            command
          end

          def view_options
            @view_options ||= (opts = view.options.dup
                               opts.delete(:session)
                               opts)
          end
        end
      end
    end
  end
end
