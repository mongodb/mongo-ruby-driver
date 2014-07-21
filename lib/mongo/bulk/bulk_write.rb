# Copyright (C) 2009-2014 MongoDB, Inc.
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
  module Bulk
    # This class keeps track of operations as they are chained on a bulk object.
    # It handles:
    #  - Merging operations before execution if the bulk write is unordered.
    #  - Execution of the operations after possibly merging them.
    #  - Bookkeeping for mapping errors to the ops as chained by the user
    #  - Processing responses for backwards compatibility
    class BulkWrite

      def initialize(collection, opts = {})
        @collection = collection
        @ordered    = !!opts[:ordered]
        @batches    = [ Batch.new(@ordered) ]
      end

      def insert(doc)
        raise Exception unless valid_doc?(doc)

        spec = { :documents => [ doc ],
                 :db_name => db_name,
                 :coll_name => coll_name }

        op = Mongo::Operation::Write::Insert.new(spec)
        current_batch << op
      end

      def find(q)
        BulkCollectionView.new(self, q)
      end

      def execute(write_concern = nil)
        current_batch.execute(write_concern ||
                              @collection.write_concern)
        @batches << Batch.new(ordered?)
      end

      def db_name
        @collection.database.name
      end

      def coll_name
        @collection.name
      end

      def ordered?
        @ordered
      end

      def push_op(op)
        current_batch << op
      end
      alias_method :<<, :push_op

      private

      def current_batch
        @batches.last
      end

      def valid_doc?(doc)
        doc.is_a?(Hash)
      end
    end

    # Handles all logic for a chain of ops between executions.
    class Batch

      def initialize(ordered)
        @ops = []
        @executed = false
        @ordered = ordered
      end

      def push_op(op)
        @ops << op
      end
      alias_method :<<, :push_op

      def execute(write_concern)
        raise Exception if @ops.empty?
        raise Exception if executed?

        # Record user-ordering of ops in this batch
        @ops.each_with_index { |op, index| op.set_order(index) }

        # @todo set this before or after the execute?
        @executed = true
        @ops = merge_ops
        # @todo: figure out how context works
        #context =
        ops = @ops.dup

        until ops.empty?
          op = ops.shift
          begin
            op.execute(write_concern)
          #rescue Exception #BSON::InvalidDocument # message too large
          #  ops = op.slice(2) + ops
          end
        end
      end

      private

      # merge ops into appropriately-sized operation messages
      def merge_ops
        if @ordered
          merge_consecutive_ops(@ops)
        else
          merge_ops_by_type(@ops)
        end
      end

      def merge_consecutive_ops(operations)
        operations.inject([]) do |memo, op|
          previous_op = memo.last
          if previous_op.class == op.class
            memo.tap do |m|
              m[m.size - 1] = previous_op.merge(op)
            end
          else
            memo << op
          end
        end
      end

      def merge_ops_by_type(operations)
        ops_by_type = operations.inject({}) do |memo, op|
          if memo[op.class]
            memo.tap do |m|
              m[op.class] << op
            end
          else
            memo.tap do |m|
              m[op.class] = [ op ]
            end
          end
        end

        ops_by_type.keys.inject([]) do |memo, type|
          memo << merge_consecutive_ops(ops_by_type[type])
        end.flatten
      end

      def executed?
        @executed
      end
    end
  end
end