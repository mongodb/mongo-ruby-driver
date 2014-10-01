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
    # @api semipublic
    #
    # @since 2.0.0
    class BulkWrite

      # @return [ Mongo::Collection ] The collection on which this bulk write
      #   operation will be executed.
      attr_reader :collection

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
        @batches    = [ Batch.new(ordered?) ]
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
        raise Exception.new("Not a valid doc") unless valid_doc?(doc)

        spec = { documents: [ doc ],
                 db_name: db_name,
                 coll_name: coll_name,
                 ordered: ordered?,
                 write_concern: @collection.write_concern 
               }

        op = Mongo::Operation::Write::BulkInsert.new(spec)
        self << op
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
        self
      end
      alias_method :<<, :push_op

      # Execute the current batch of operations.
      #
      # @params [ WriteConcern::Mode ] write_concern Optionally provide a write
      #   concern. The collection's write concern will be used as the default.
      #
      # @return [ Hash ] A document response from the server.
      def execute(write_concern = nil)
        response = current_batch.execute( self,
                                          write_concern ||
                                          @collection.write_concern )
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

      # Whether this bulk object will send the operations in the order in which
      # they are chained, and whether the server will apply operations in the order
      # in which they were sent.
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
        doc.respond_to?(:keys)
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
        self
      end
      alias_method :<<, :push_op

      # Execute this batch of operations.
      #
      # @params [ WriteConcern::Mode ] write_concern The write concern to use for
      #   this batch execution of operations.
      #
      # @raise [ BSON::InvalidDocument ] The document must not exceed the max message
      #   size.
      #
      # @return [ Hash ] A document response from the server.
      def execute(bulk_write, write_concern = nil)
        raise Exception.new("No ops to execute") if @ops.empty?
        raise Exception.new("Bulk object already executed") if executed?

        # @todo: Record user-ordering of ops in this batch
        #@ops.each_with_index { |op, index| op.set_order(index) }

        # @todo set this before or after the execute?
        @executed = true
        ops = merge_ops

        replies = []
        until ops.empty?
          op = ops.shift

          until op.valid_batch_size?(bulk_write.collection.
                                      next_primary.context.max_write_batch_size)
            ops = op.batch(2) + ops
            op = ops.shift
          end

          op = op.write_concern(write_concern) if write_concern
          replies << op.execute(bulk_write.collection.next_primary.context)
          return make_response(replies) if stop_executing?(replies.last)
        end
        make_response(replies)# if write_concern.gle
      end

      private

      def stop_executing?(reply)
        ordered? && reply.write_failure?
      end

      # Whether this batch has already been executed.
      #
      # @return [ true, false ] Whether this batch has been executed.
      def executed?
        @executed
      end

      # Whether ops should be executed in order.
      #
      # @return [ true, false ] Whether this batch is ordered.
      def ordered?
        @ordered
      end

      # If an ordered bulk object, merge adjacent same-type operations.
      # If an unordered bulk object, merge same-type operations.
      #
      # @return [ Array ] List of merged operations.
      def merge_ops
        if ordered?
          merge_consecutive_ops(@ops)
        else
          merge_ops_by_type(@ops)
        end
      end

      # Merge consecutive operations of the same type.
      #
      # @return [ Array ] List of merged operations.
      def merge_consecutive_ops(operations)
        operations.inject([]) do |merged_ops, op|
          previous_op = merged_ops.last
          if previous_op.class == op.class
            merged_ops.tap do |m|
              m[m.size - 1] = previous_op.merge!(op)
            end
          else
            merged_ops << op
          end
        end
      end

      # Merge all operations of the same type.
      # Original order is not guaranteed.
      #
      # @return [ Array ] List of merged operations by type.
      def merge_ops_by_type(operations)
        ops_by_type = operations.inject({}) do |merged_ops, op|
          if merged_ops[op.class]
            merged_ops.tap do |m|
              m[op.class] << op
            end
          else
            merged_ops.tap do |m|
              m[op.class] = [ op ]
            end
          end
        end

        ops_by_type.keys.inject([]) do |merged_ops, type|
          merged_ops << merge_consecutive_ops( ops_by_type[ type ] )
        end.flatten
      end

      # Process the response from the server after the bulk execution.
      #
      # @return [ Hash ] The response from the server.
      def make_response(results)
        { 'writeErrors' => [],
          'writeConcernErrors' => [],
          'nInserted' => nil,
          'nUpserted' => nil,
          'nMatched'  => nil, # is equivalent to the "n" field in the getLastError response after a legacy update
          'nModified' => nil, # nModified is incremented only when an update operation actually changes a document, nil for legacy
          'nRemoved'  => nil,
          'upserted'  => [] }.tap do |response|
          results.map do |result|
            response['nInserted'] = ( response['nInserted'] || 0 ) + result.n_inserted if result.respond_to?(:n_inserted)
            response['nMatched'] = ( response['nMatched'] || 0 ) + result.n_matched if result.respond_to?(:n_matched)
            response['nModified'] = ( response['nModified'] || 0 ) + result.n_modified if result.respond_to?(:n_modified) && result.n_modified
            response['nUpserted'] = ( response['nUpserted'] || 0 ) + result.n_upserted if result.respond_to?(:n_upserted)
            response['nRemoved'] = ( response['nRemoved'] || 0 ) + result.n_removed if result.respond_to?(:n_removed)
          end
        end
      end
    end
  end
end
