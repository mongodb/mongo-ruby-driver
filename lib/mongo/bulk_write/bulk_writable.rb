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

module Mongo
  module BulkWrite
    module BulkWritable
      include Insertable
      include Deletable
      include Updatable
      extend Forwardable

      def_delegators :@collection, :database, :cluster, :next_primary

      def initialize(collection, operations, options)
        @collection = collection
        @operations = operations
        @options = options
      end

      private

      def merge_consecutive_ops(ops)
        ops.inject([]) do |merged, op|
          type = op.keys.first
          previous = merged.last
          if previous && previous.keys.first == type
            merged[-1].merge(type => previous[type] << op[type])
            merged
          else
            merged << { type => [ op[type] ].flatten }
          end
        end
      end

      def merge_ops_by_type
        [ @operations.inject({}) do |merged, op|
          type = op.keys.first
          merged.merge!(type => merged.fetch(type, []).push(op[type]))
        end ]
      end

      def batched_operation(op, server)
        type = op.keys.first
        ops = []
        until op[type].size < server.max_write_batch_size
          ops << { type => op[type].slice!(0, server.max_write_batch_size) }
        end
        ops << op
      end

      def write_concern
        @write_concern ||= WriteConcern.get(@options[:write_concern]) ||
                            @collection.write_concern
      end

      def process(result)
        @results ||= {}
        write_errors = result.aggregate_write_errors

        [:n_inserted, :n_removed, :n_modified, :n_upserted, :n_matched].each do |count|
          @results.merge!(
          count => (@results[count] || 0) + result.send(count)
        ) if result.respond_to?(count) 
        end

        @results.merge!(
          'writeErrors' => ((@results['writeErrors'] || []) << write_errors).flatten
        ) if write_errors
        @results
      end

      def valid_doc?(doc)
        doc.respond_to?(:keys)
      end

      def validate_type!(type, op)
        raise Error::InvalidBulkOperation.new(type, op) unless respond_to?(type, true)
      end

      def validate_operations!
        unless @operations && @operations.size > 0
          raise ArgumentError.new('Operations cannot be empty')
        end
      end
    end
  end
end