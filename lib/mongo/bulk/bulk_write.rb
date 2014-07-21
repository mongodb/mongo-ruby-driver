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

    # This semipublic class keeps track of operations as they are chained
    # onto a bulk object. It is created by a collection using either the
    # #initialize_unordered_bulk_op or #initialize_ordered_bulk_op methods.
    #
    # Its responsibilities include:
    #  - Keeping track of ops defined between batch executions.
    #  - Merging operations before execution.
    #  - Execution of the operations.
    #  - Bookkeeping for mapping errors to their source operations.
    #  - Processing responses for backwards compatibility.
    #
    # @note The +BulkWrite+ API is semipublic.
    # @api semipublic
    class BulkWrite

      # Initialize the Bulk object.
      #
      # @example Create an ordered bulk object.
      #   BulkWrite.new(collection, :ordered => true)
      #
      # @example Create an unordered bulk object.
      #   BulkWrite.new(collection, :ordered => false)
      #
      # @params [ Collection ] collection The collection on which the batch
      #   operations will be executed.
      def initialize(collection, opts = {})
        @collection = collection
        @ordered    = !!opts[:ordered]
        @batches    = [ Batch.new(@ordered) ]
      end

      # Insert a document into the collection.
      #
      # @params [ Hash ] doc The document to insert.
      #
      # @todo: change this Exception class
      # @raise [ Exception ] The document must be a Hash.
      #
      # @return [ Batch ] The current batch object.
      def insert(doc)
        raise Exception unless valid_doc?(doc)

        spec = { :documents => [ doc ],
                 :db_name => db_name,
                 :coll_name => coll_name }

        op = Mongo::Operation::Write::Insert.new(spec)
        current_batch << op
      end

      # Define a query selector.
      #
      # @params [ Hash ] q The selector.
      #
      # @return [ BulkCollectionView ] A new BulkCollectionView object
      #   representing an operation requiring a selector.
      def find(q)
        BulkCollectionView.new(self, q)
      end

      # Push a new operation onto this bulk object's current batch.
      #
      # @params [ Operation ] op The operation to push onto this bulk object.
      #
      # @return [ Batch ] The current batch object.
      def push_op(op)
        current_batch << op
      end
      alias_method :<<, :push_op

      # Execute the current batch of operations.
      #
      # @params [ WriteConcern::Mode ] write_concern The write concern to use
      #   for this batch execution.
      #
      # @return [ Hash ] A document response from the server.
      def execute(write_concern = nil)
        response = current_batch.execute(write_concern ||
                              @collection.write_concern)
        @batches << Batch.new(ordered?)
        response
      end

      # The name of the database containing the collection.
      #
      # @return [ String ] The name of the database.
      def db_name
        @collection.database.name
      end

      # The name of the collection on which these operations will be executed.
      #
      # @return [ String ] The collection name.
      def coll_name
        @collection.name
      end

      # Whether this bulk object will execute the operations in the order in which
      # they are chained.
      #
      # @return [ true, false ] Whether the operations will be executed in order.
      def ordered?
        @ordered
      end

      private

      # The current batch of operations.
      #
      # @return [ Batch ] The current batch.
      def current_batch
        @batches.last
      end

      # Whether the object is a valid document.
      # Must be a Hash.
      #
      # @return [ true, false ] Whether the object is a valid document.
      def valid_doc?(doc)
        doc.is_a?(Hash)
      end
    end

    # Encapsulates all logic for a chain of operations between executions.
    #
    # @note The +Batch+ API is semipublic and should only be used by a
    # BulkWrite object.
    #
    # @api semipublic
    class Batch

      # Initialize a Batch object.
      #
      # @params [ true, false ] Whether the operations should be executed in the
      #   order in which they were chained.
      def initialize(ordered)
        @ops = []
        @executed = false
        @ordered = ordered
      end

      # Push a new operation onto this batch object.
      #
      # @params [ Operation ] op The operation to push onto this batch object.
      #
      # @return [ Batch ] The current batch object.
      def push_op(op)
        @ops << op
      end
      alias_method :<<, :push_op

      # Execute this batch of operations.
      #
      # @params [ WriteConcern::Mode ] write_concern The write concern to use for
      #   this execution of operations.
      #
      # @raise [ BSON::InvalidDocument ] The document must not exceed the max message
      #   size.
      #
      # @return [ Hash ] A document response from the server.
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
            # @todo: uncomment this when collection is finished.
            process_response(op.execute(write_concern))
          rescue Exception #BSON::InvalidDocument # message too large
            ops = op.slice(2) + ops
          end
        end
      end

      private

      # Whether this batch has already been executed.
      #
      # @return [ true, false ] Whether this batch has been executed.
      def executed?
        @executed
      end

      # If an ordered bulk object, merge adjacent same-type operations.
      # If an unordered bulk object, merge same-type operations.
      #
      # @return [ Array ] List of merged operations.
      def merge_ops
        if @ordered
          merge_consecutive_ops(@ops)
        else
          merge_ops_by_type(@ops)
        end
      end

      # Merge consecutive operations of the same type.
      #
      # @return [ Array ] List of merged operations.
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

      # Merge all operations of the same type. Original order is not
      # guaranteed.
      #
      # @return [ Array ] List of merged operations by type.
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

      # Process the response from the server after the bulk execution.
      #
      # @return [ Hash ] The response from the server.
      def process_response(response)
      end
    end
  end
end