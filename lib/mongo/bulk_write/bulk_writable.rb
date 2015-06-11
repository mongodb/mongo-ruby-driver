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

require 'mongo/bulk_write/insertable'
require 'mongo/bulk_write/deletable'
require 'mongo/bulk_write/updatable'
require 'mongo/bulk_write/replacable'

module Mongo
  module BulkWrite

    # Defines shared behaviour between ordered and unordered bulk operations.
    #
    # @since 2.0.0
    module BulkWritable
      include Insertable
      include Deletable
      include Updatable
      include Replacable
      extend Forwardable

      # Delegate various methods to the collection.
      def_delegators :@collection, :database, :cluster, :next_primary

      # The fields contained in the result document returned from executing the
      # operations.
      #
      # @since 2.0.0.
      RESULT_FIELDS = [ :n_inserted,
                        :n_removed,
                        :n_modified,
                        :n_upserted,
                        :n_matched ]


      # Initialize a bulk write object.
      #
      # @example Initialize a bulk write object.
      #   Mongo::BulkWrite::OrderedBulkWrite.new(collection, operations, options)
      #   Mongo::BulkWrite::UnorderedBulkWrite.new(collection, operations, options)
      #
      # @param [ Mongo::Collection ] collection The collection the operations will
      #   be executed on.
      # @param [ Array<Hash> ] operations The operations to be executed.
      # @param [ Hash ] options The options.
      #
      # @option options [ Hash ] :write_concern The write concern to use for this
      #   bulk write.
      #
      # @since 2.0.0
      def initialize(collection, operations, options)
        @collection = collection
        @operations = operations
        @options = options
      end

      # Execute the bulk operations.
      #
      # @example Execute the operations.
      #   bulk.execute
      #
      # @return [ Hash ] The results from the bulk write.
      #
      # @since 2.0.0
      def execute
        server = next_primary
        validate_operations!
        merged_ops.each do |op|
          validate_type!(op.keys.first)
          execute_op(op, server)
        end
        finalize
      end

      private

      def valid_doc?(doc)
        doc.respond_to?(:keys) ||
          doc.respond_to?(:document)
      end

      def write_concern
        @write_concern ||= WriteConcern.get(@options[:write_concern]) ||
                            @collection.write_concern
      end

      def validate_operations!
        unless @operations && @operations.size > 0
          raise ArgumentError.new('Operations cannot be empty')
        end
      end

      def validate_type!(type)
        unless respond_to?(type, true)
          raise Error::InvalidBulkOperationType.new(type)
        end
      end

      def max_write_batches(op, server)
        type = op.keys.first
        ops = []
        while op[type].size > server.max_write_batch_size
          ops << { type => op[type].shift(server.max_write_batch_size),
                   :indexes => op[:indexes].shift(server.max_write_batch_size) }
        end
        ops << op
      end

      def split(op, type)
        n = op[type].size/2
        [ { type => op[type].shift(n),
            :indexes => op[:indexes].shift(n) },
          { type => op[type],
            :indexes => op[:indexes] }
        ]
      end

      def execute_op(operation, server)
        ops = max_write_batches(operation, server)

        until ops.empty?
          op = ops.shift
          type = op.keys.first
          begin
            process(send(type, op, server), op[:indexes])
          rescue Error::MaxBSONSize, Error::MaxMessageSize => ex
            raise ex if op[type].size < 2
            ops = split(op, type) + ops
          end
        end
      end

      def merge_consecutive_ops(ops)
        ops.each_with_index.inject([]) do |merged, (op, i)|
          type = op.keys.first
          op[:indexes] ||= [ i ]
          previous = merged.last
          if previous && previous.keys.first == type
            merged[-1].merge!(type => previous[type] << op[type],
                             :indexes => previous[:indexes] + op[:indexes])
            merged
          else
            merged << { type => [ op[type] ].flatten,
                        :indexes => op[:indexes] }
          end
        end
      end

      def merge_ops_by_type
        indexes = {}
        ops_hash = @operations.each_with_index.inject({}) do |merged, (op, i)|
          type = op.keys.first
          merged.merge!(op) { |type, v1, v2| ([v1] << v2).flatten }
          indexes[type] = (indexes[type] || []).push(i)
          merged
        end
        ops_hash.keys.reduce([]) do |ops_list, type|
          ops_list << { type => ops_hash[type], :indexes => indexes[type] }
        end
      end

      def combine_results(result, indexes)
        @results ||= {}
        write_errors = result.aggregate_write_errors(indexes)

        # The Bulk API only returns the first write concern error encountered.
        @write_concern_errors ||= result.aggregate_write_concern_errors(indexes)

        @results.tap do |results|
          RESULT_FIELDS.each do |field|
            results.merge!(
              field => (results[field] || 0) + result.send(field)
            ) if result.respond_to?(field)
          end

          results.merge!(
            write_errors: ((results[:write_errors] || []) << write_errors).flatten
          ) if write_errors

          results.merge!(write_concern_errors: @write_concern_errors) if @write_concern_errors
        end
      end
    end
  end
end