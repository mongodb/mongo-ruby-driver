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
    # This class keeps track of the operations as they are pushed onto itself.
    # It handles:
    #  - Merging operations before execution if the bulk write is unordered.
    #  - Execution of the operations after possibly merging them.
    #  - Bookkeeping for counts and such
    #  - Processing responses and behavior for backwards compatibility
    class BulkWrite

      def initialize(collection, opts = {})
        @collection = collection
        @ordered = !!opts[:ordered]
        @batches = [ Batch.new ]
      end

      def insert(doc)
        raise Exception unless valid_doc?(doc)

        spec = { :documents => doc,
                 :db_name => db_name,
                 :coll_name => coll_name }

        op = Mongo::Operation::Write::Insert.new(spec)
        current_batch << op
      end

      def find(q)
        BulkCollectionView.new(self, q)
      end

      def execute(write_concern = nil)
        current_batch.execute(ordered?,
                            write_concern || @collection.write_concern)
        @batches << Batch.new
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

    class Batch

      def initialize
        @ops = []
        @executed = false
      end

      def push_op(op)
        @ops << op
      end
      alias_method :<<, :push_op

      def execute(ordered, write_concern)
        raise Exception if @ops.empty?
        raise Exception if @executed

        @executed = true
        prepare_ops(ordered)
        @ops.each do |op|
          response = op.execute(wc)
          # stuff for bookkeeping
        end
      end

      private

      # merge ops into appropriately-sized operation messages
      def prepare_ops(ordered)
        if ordered
          ordered_merge
        else
          unordered_merge
        end
      end

      def ordered_merge
        @ops.inject([]) do |memo, op|
          previous_op = memo.last
          if previous_op.class == op.class && !previous_op.at_max_size?
            memo << previous_op.merge(op)
          else
            memo << op
          end
        end
      end

      def unordered_merge
        @ops.inject({}) do |memo, op|
          if memo[op.class]
            memo[op.class] << op
          else
            memo[op.class] = [ op ]
          end
        end
      end
    end
  end
end