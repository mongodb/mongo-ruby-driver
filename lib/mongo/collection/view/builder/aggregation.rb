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

        # Builds an aggregation command specification from the view and options.
        #
        # @since 2.2.0
        class Aggregation
          extend Forwardable

          # The mappings from ruby options to the aggregation options.
          #
          # @since 2.2.0
          MAPPINGS = BSON::Document.new(
            :allow_disk_use => 'allowDiskUse',
            :max_time_ms => 'maxTimeMS',
            :explain => 'explain',
            :bypass_document_validation => 'bypassDocumentValidation',
            :collation => 'collation',
            :hint => 'hint'
          ).freeze

          def_delegators :@view, :collection, :database, :read, :write_concern

          # @return [ Array<Hash> ] pipeline The pipeline.
          attr_reader :pipeline

          # @return [ Collection::View ] view The collection view.
          attr_reader :view

          # @return [ Hash ] options The map/reduce specific options.
          attr_reader :options

          # Initialize the builder.
          #
          # @example Initialize the builder.
          #   Aggregation.new(map, reduce, view, options)
          #
          # @param [ Array<Hash> ] pipeline The aggregation pipeline.
          # @param [ Collection::View ] view The collection view.
          # @param [ Hash ] options The map/reduce options.
          #
          # @since 2.2.0
          def initialize(pipeline, view, options)
            @pipeline = pipeline
            @view = view
            @options = options
          end

          # Get the specification to pass to the aggregation operation.
          #
          # @example Get the specification.
          #   builder.specification
          #
          # @return [ Hash ] The specification.
          #
          # @since 2.2.0
          def specification
            spec = {
                    selector: aggregation_command,
                    db_name: database.name,
                    read: read
                   }
            write? ? spec.merge!(write_concern: write_concern) : spec
          end

          private

          def write?
            pipeline.any? { |operator| operator[:$out] || operator['$out'] }
          end

          def aggregation_command
            command = BSON::Document.new(:aggregate => collection.name, :pipeline => pipeline)
            command[:cursor] = cursor if cursor
            command[:readConcern] = collection.read_concern if collection.read_concern
            command.merge!(Options::Mapper.transform_documents(options, MAPPINGS))
            command
          end

          def cursor
            if options[:use_cursor] == true || options[:use_cursor].nil?
              batch_size_doc
            end
          end

          def batch_size_doc
            (value = options[:batch_size] || view.batch_size) ?  { :batchSize => value } : {}
          end
        end
      end
    end
  end
end
