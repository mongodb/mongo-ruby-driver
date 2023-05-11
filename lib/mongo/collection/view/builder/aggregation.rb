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

        # Builds an aggregation command specification from the view and options.
        #
        # @since 2.2.0
        class Aggregation
          extend Forwardable

          # The mappings from ruby options to the aggregation options.
          #
          # @since 2.2.0
          MAPPINGS = BSON::Document.new(
            allow_disk_use: 'allowDiskUse',
            bypass_document_validation: 'bypassDocumentValidation',
            explain: 'explain',
            collation: 'collation',
            comment: 'comment',
            hint: 'hint',
            let: 'let',
            # This is intentional; max_await_time_ms is an alias for maxTimeMS
            # used on getMore commands for change streams.
            max_await_time_ms: 'maxTimeMS',
            max_time_ms: 'maxTimeMS',
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
          # @param [ Array<Hash> ] pipeline The aggregation pipeline.
          # @param [ Collection::View ] view The collection view.
          # @param [ Hash ] options The map/reduce and read preference options.
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
              read: @options[:read_preference] || view.read_preference,
              session: @options[:session],
              collation: @options[:collation],
            }
            if write?
              spec.update(write_concern: write_concern)
            end
            spec
          end

          private

          def write?
            pipeline.any? do |operator|
              operator[:$out] || operator['$out'] ||
              operator[:$merge] || operator['$merge']
            end
          end

          def aggregation_command
            command = BSON::Document.new
            # aggregate must be the first key in the command document
            if view.is_a?(Collection::View)
              command[:aggregate] = collection.name
            elsif view.is_a?(Database::View)
              command[:aggregate] = 1
            else
              raise ArgumentError, "Unknown view class: #{view}"
            end
            command[:pipeline] = pipeline
            if read_concern = view.read_concern
              command[:readConcern] = Options::Mapper.transform_values_to_strings(
                read_concern)
            end
            command[:cursor] = cursor if cursor
            command.merge!(Options::Mapper.transform_documents(options, MAPPINGS))
            command
          end

          def cursor
            if options[:use_cursor] == true || options[:use_cursor].nil?
              batch_size_doc
            end
          end

          def batch_size_doc
            value = options[:batch_size] || view.batch_size
            if value == 0 && write?
              {}
            elsif value
              { :batchSize => value }
            else
              {}
            end
          end
        end
      end
    end
  end
end
