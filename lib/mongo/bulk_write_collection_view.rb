# Copyright (C) 2009-2013 MongoDB, Inc.
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

  # A bulk write view to a collection of documents in a database.
  class BulkWriteCollectionView
    include Mongo::WriteConcern

    DEFAULT_OP_ARGS = {:q => {}}
    MULTIPLE_ERRORS_OCCURRED = 65 # MongoDB Core Server mongo/base/error_codes.err MultipleErrorsOccurred
    MULTIPLE_ERRORS_OCCURRED_ERRMSG = "batch item errors occurred"

    attr_reader :collection, :options, :ops, :op_args

    # Initialize a bulk-write-view object to a collection with default query selector {}.
    #
    # A bulk write operation is initialized from a collection object.
    # For example, for an ordered bulk write view:
    #
    #   bulk = collection.initialize_ordered_bulk_op
    #
    # or for an unordered bulk write view:
    #
    #   bulk = collection.initialize_unordered_bulk_op
    #
    # The bulk write view collects individual write operations together so that they can be
    # executed as a batch for significant performance gains.
    # The ordered bulk operation will execute each operation serially in order.
    # Execution will stop at the first occurrence of an error for an ordered bulk operation.
    # The unordered bulk operation will be executed and may take advantage of parallelism.
    # There are no guarantees for the order of execution of the operations on the server.
    # Execution will continue even if there are errors for an unordered bulk operation.
    # While the API supports mixing of write operation types in a bulk operation,
    # currently only contiguous commands of the same type are submitted as a batch and
    # benefit from performance gains.
    #
    # A modify method sets a value on the current object.
    # A set methods returns a duplicate of the current object with a value set.
    # A terminator write method appends a write operation to the bulk batch collected in the view.
    #
    # @param [Collection] collection the parent collection object
    #
    # @option opts [Boolean] :ordered (true) Set bulk execution for ordered or unordered
    #
    # @return [BulkWriteCollectionView]
    def initialize(collection, options = {})
      @collection = collection
      @options = options
      @ops = []
      @op_args = DEFAULT_OP_ARGS.dup
    end

    def inspect
      vars = [:@options, :@ops, :@op_args]
      vars_inspect = vars.collect{|var| "#{var}=#{instance_variable_get(var).inspect}"}
      "#<Mongo::BulkWriteCollectionView:0x#{self.object_id} " <<
      "@collection=#<Mongo::Collection:0x#{@collection.object_id}>, #{vars_inspect.join(', ')}>"
    end

    # Modify the query selector for subsequent bulk write operations.
    # The default query selector on creation of the bulk write view is {}.
    #
    # @param [Hash] q the query selector
    #
    # @return [BulkWriteCollectionView]
    def find(q)
      op_args_set(:q, q)
    end

    # Modify the upsert option argument for subsequent bulk write operations.
    #
    # @param [Boolean] value (true) the upsert option value
    #
    # @return [BulkWriteCollectionView]
    def upsert!(value = true)
      op_args_set(:upsert, value)
    end

    # Set the upsert option argument for subsequent bulk write operations.
    #
    # @param [Boolean] value (true) the upsert option value
    #
    # @return [BulkWriteCollectionView] a duplicated object
    def upsert(value = true)
      dup.upsert!(value)
    end

    # Update one document matching the selector.
    #
    #   bulk.find({"a" => 1}).update_one({"$inc" => {"x" => 1}})
    #
    # Use the upsert! or upsert method to specify an upsert. For example:
    #
    #   bulk.find({"a" => 1}).upsert.updateOne({"$inc" => {"x" => 1}})
    #
    # @param [Hash] u the update document
    #
    # @return [BulkWriteCollectionView]
    def update_one(u)
      raise MongoArgumentError, "document must start with an operator" unless update_doc?(u)
      op_push([:update, @op_args.merge(:u => u, :multi => false)])
    end

    # Update all documents matching the selector. For example:
    #
    #   bulk.find({"a" => 2}).update({"$inc" => {"x" => 2}})
    #
    # Use the upsert! or upsert method to specify an upsert.  For example:
    #
    #   bulk.find({"a" => 2}).upsert.update({"$inc" => {"x" => 2}})
    #
    # @param [Hash] u the update document
    #
    # @return [BulkWriteCollectionView]
    def update(u)
      raise MongoArgumentError, "document must start with an operator" unless update_doc?(u)
      op_push([:update, @op_args.merge(:u => u, :multi => true)])
    end

    # Replace entire document (update with whole doc replace). For example:
    #
    #   bulk.find({"a" => 3}).replace_one({"x" => 3})
    #
    # @param [Hash] u the replacement document
    #
    # @return [BulkWriteCollectionView]
    def replace_one(u)
      raise MongoArgumentError, "document must not contain any operators" unless replace_doc?(u)
      op_push([:update, @op_args.merge(:u => u, :multi => false)])
    end

    # Remove a single document matching the selector. For example:
    #
    #   bulk.find({"a" => 4}).remove_one;
    #
    # @return [BulkWriteCollectionView]
    def remove_one
      op_push([:delete, @op_args.merge(:limit => 1)])
    end

    # Remove all documents matching the selector. For example:
    #
    #   bulk.find({"a" => 5}).remove;
    #
    # @return [BulkWriteCollectionView]
    def remove
      op_push([:delete, @op_args.merge(:limit => 0)])
    end

    # Insert a document. For example:
    #
    #   bulk.insert({"x" => 4})
    #
    # @return [BulkWriteCollectionView]
    def insert(document)
      # TODO - check keys
      op_push([:insert, {:d => document}])
    end

    # Execute the bulk operation, with an optional write concern overwriting the default w:1.
    # For example:
    #
    #   write_concern = {:w => 1, :j => 1}
    #   bulk.execute({write_concern})
    #
    # On return from execute, the bulk operation is cleared,
    # but the selector and upsert settings are preserved.
    #
    # @return [BulkWriteCollectionView]
    def execute(opts = {})
      write_concern = get_write_concern(opts, @collection)
      @ops.each_with_index{|op, index| op.last.merge!(:ord => index)} # infuse ordinal here to avoid issues with upsert
      if @collection.use_write_command?(write_concern)
        errors, exchanges = @collection.command_writer.bulk_execute(@ops, @options, opts)
      else
        errors, exchanges = @collection.operation_writer.bulk_execute(@ops, @options, opts)
      end
      @ops = []
      raise BulkWriteError.new(MULTIPLE_ERRORS_OCCURRED_ERRMSG, MULTIPLE_ERRORS_OCCURRED, {:errors => errors, :exchanges => exchanges}) unless errors.empty?
      [errors, exchanges]
    end

    private

    def initialize_copy(other)
      other.instance_variable_set(:@options, other.options.dup)
    end

    def op_args_set(op, value)
      @op_args[op] = value
      self
    end

    def op_push(op)
      @ops << op
      self
    end

    def update_doc?(doc)
      !doc.empty? && doc.keys.first.to_s =~ /^\$/
    end

    def replace_doc?(doc)
      doc.keys.all?{|key| key !~ /^\$/}
    end

  end

  class Collection

    # Initialize an ordered bulk write view for this collection
    # Execution will stop at the first occurrence of an error for an ordered bulk operation.
    #
    # @return [BulkWriteCollectionView]
    def initialize_ordered_bulk_op
      BulkWriteCollectionView.new(self, :ordered => true)
    end

    # Initialize an unordered bulk write view for this collection
    # The unordered bulk operation will be executed and may take advantage of parallelism.
    # There are no guarantees for the order of execution of the operations on the server.
    # Execution will continue even if there are errors for an unordered bulk operation.
    #
    # @return [BulkWriteCollectionView]
    def initialize_unordered_bulk_op
      BulkWriteCollectionView.new(self, :ordered => false)
    end

  end

end
