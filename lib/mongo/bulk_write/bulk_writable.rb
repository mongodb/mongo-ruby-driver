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
    module BulkWritable
      include Insertable
      include Deletable
      include Updatable
      include Replacable
      extend Forwardable

      def_delegators :@collection, :database, :cluster, :next_primary

      RESULT_FIELDS = [ :n_inserted,
                        :n_removed,
                        :n_modified,
                        :n_upserted,
                        :n_matched ]

      def initialize(collection, operations, options)
        @collection = collection
        @operations = operations
        @options = options
      end

      def execute
        validate_operations!
        merged_ops.each do |op|
          execute_op(op)
        end
        finalize
      end

      private

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

      def batches(op, server)
        type = op.keys.first
        ops = []
        until op[type].size < server.max_write_batch_size
          ops << { type => op[type].slice!(0, server.max_write_batch_size),
                   :indexes => op[:indexes].slice!(0, server.max_write_batch_size) }
        end
        ops << op
      end

      def execute_op(operation)
        server = next_primary
        type = operation.keys.first
        validate_type!(type)
        batches(operation, server).each do |op|
          process(send(type, op, server), op[:indexes])
        end
      end

      def merge_consecutive_ops(ops)
        ops.each_with_index.inject([]) do |merged, (op, i)|
          type = op.keys.first
          op[:indexes] = [ i ] unless op[:indexes]
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
        [ @operations.inject({}) do |merged, op|
          type = op.keys.first
          merged.merge!(type => merged.fetch(type, []).push(op[type]))
        end ]
      end

      def write_concern
        @write_concern ||= WriteConcern.get(@options[:write_concern]) ||
                            @collection.write_concern
      end

      def merge_result(result, indexes)
        @results ||= {}
        write_errors = result.aggregate_write_errors(indexes)
        write_concern_errors = result.aggregate_write_concern_errors

        @results.tap do |results|

          RESULT_FIELDS.each do |field|
            results.merge!(
              field => (results[field] || 0) + result.send(field)
            ) if result.respond_to?(field)
          end

          results.merge!(
            write_errors: ((results[:write_errors] || []) << write_errors).flatten
          ) if write_errors

          results.merge!(
            write_concern_errors: ((results[:write_concern_errors] || []) << write_concern_errors).flatten
          ) if write_concern_errors
        end
      end
    end
  end
end