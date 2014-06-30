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
        @ops = []
        @write_concern = collection.write_concern
      end

      def insert(doc)
        raise Exception unless valid_doc?(doc)
        spec = { :documents => doc,
                 :db_name => db_name,
                 :coll_name => coll_name,
                 :write_concern => @write_concern }
        op = Mongo::Operation::Write::Insert.new(spec)
        push_op(op)
      end

      def find(q)
        BulkCollectionView.new(self, q)
      end

      def write_concern(write_concern)
        @write_concern = write_concern
        self
      end

      def get_write_concern
        @write_concern
      end

      def execute(opts = {})
        merge_ops unless ordered?
        wc = opts[:write_concern] ? opts[:write_concern] : @write_concern
        merge_ops if @ordered
        @ops.each do |op|
          response = op.execute(wc)
          # stuff for bookkeeping
        end
      end

      def push_op(op)
        @ops << op
        self
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

      private

      # merge ops into appropriately-sized operation messages
      def merge_ops
      end

      def valid_doc?(doc)
        doc.is_a?(Hash)
      end
    end
  end
end