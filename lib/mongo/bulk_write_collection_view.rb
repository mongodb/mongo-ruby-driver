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

    DEFAULT_OP_ARGS = {:q => nil}
    MULTIPLE_ERRORS_MSG = "batch item errors occurred"
    EMPTY_BATCH_MSG = "batch is empty"

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
    #
    # A bulk operation is programmed as a sequence of individual operations.
    # An individual operation is composed of a method chain of modifiers or setters terminated by a write method.
    # A modify method sets a value on the current object.
    # A set methods returns a duplicate of the current object with a value set.
    # A terminator write method appends a write operation to the bulk batch collected in the view.
    #
    # The API supports mixing of write operation types in a bulk operation.
    # However, server support affects the implementation and performance of bulk operations.
    #
    # MongoDB version 2.6 servers currently support only bulk commands of the same type.
    # With an ordered bulk operation,
    # contiguous individual ops of the same type can be batched into the same db request,
    # and the next op of a different type must be sent separately in the next request.
    # Performance will improve if you can arrange your ops to reduce the number of db requests.
    # With an unordered bulk operation,
    # individual ops can be grouped by type and sent in at most three requests,
    # one each per insert, update, or delete.
    #
    # MongoDB pre-version 2.6 servers do not support bulk write commands.
    # The bulk operation must be sent one request per individual op.
    # This also applies to inserts in order to have accurate counts and error reporting.
    #
    #   Important note on pre-2.6 performance:
    #     Performance is very poor compared to version 2.6.
    #     We recommend bulk operation with pre-2.6 only for compatibility or
    #     for development in preparation for version 2.6.
    #     For better performance with pre-version 2.6, use bulk insertion with Collection#insert.
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
    # For operations that require a query selector, find() must be set
    # per operation, or set once for all operations on the bulk object.
    # For example, these operations:
    #
    #   bulk.find({"a" => 2}).update({"$inc" => {"x" => 2}})
    #   bulk.find({"a" => 2}).update({"$set" => {"b" => 3}})
    #
    # may be rewritten as:
    #
    #   bulk = find({"a" => 2})
    #   bulk.update({"$inc" => {"x" => 2}})
    #   bulk.update({"$set" => {"b" => 3}})
    #
    # Note that modifying the query selector in this way will not affect
    # operations that do not use a query selector, like insert().
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
      raise MongoArgumentError, EMPTY_BATCH_MSG if @ops.empty?
      write_concern = get_write_concern(opts, @collection)
      @ops.each_with_index{|op, index| op.last.merge!(:ord => index)} # infuse ordinal here to avoid issues with upsert
      if @collection.db.connection.use_write_command?(write_concern)
        errors, write_concern_errors, exchanges = @collection.command_writer.bulk_execute(@ops, @options, opts)
      else
        errors, write_concern_errors, exchanges = @collection.operation_writer.bulk_execute(@ops, @options, opts)
      end
      @ops = []
      return true if errors.empty? && (exchanges.empty? || exchanges.first[:response] == true) # w 0 without GLE
      result = merge_result(errors + write_concern_errors, exchanges)
      raise BulkWriteError.new(MULTIPLE_ERRORS_MSG, Mongo::ErrorCode::MULTIPLE_ERRORS_OCCURRED, result) if !errors.empty? || !write_concern_errors.empty?
      result
    end

    private

    def hash_except(h, *keys)
      keys.each { |key| h.delete(key) }
      h
    end

    def hash_select(h, *keys)
      Hash[*keys.zip(h.values_at(*keys)).flatten]
    end

    def tally(h, key, n)
      h[key] = h.fetch(key, 0) + n
    end

    def nil_tally(h, key, n)
      if !h.has_key?(key)
        h[key] = n
      elsif h[key]
        h[key] = n ? h[key] + n : n
      end
    end

    def append(h, key, obj)
      h[key] = h.fetch(key, []) << obj
    end

    def concat(h, key, a)
      h[key] = h.fetch(key, []) + a
    end

    def merge_index(h, exchange)
      h.merge("index" => exchange[:batch][h.fetch("index", 0)][:ord])
    end

    def merge_indexes(a, exchange)
      a.collect{|h| merge_index(h, exchange)}
    end

    def merge_result(errors, exchanges)
      ok = 0
      result = {"ok" => 0, "n" => 0}
      unless errors.empty?
        unless (writeErrors = errors.select { |error| error.class != Mongo::OperationFailure && error.class != WriteConcernError }).empty? # assignment
          concat(result, "writeErrors",
                 writeErrors.collect { |error|
                   {"index" => error.result[:ord], "code" => error.error_code, "errmsg" => error.result[:error].message}
                 })
        end
        result.merge!("code" => Mongo::ErrorCode::MULTIPLE_ERRORS_OCCURRED, "errmsg" => MULTIPLE_ERRORS_MSG)
      end
      exchanges.each do |exchange|
        response = exchange[:response]
        next unless response
        ok += response["ok"].to_i
        n = response["n"] || 0
        op_type = exchange[:op_type]
        if op_type == :insert
          n = 1 if response.key?("err") && (response["err"].nil? || response["err"] == "norepl" || response["err"] == "timeout") # OP_INSERT override n = 0 bug, n = exchange[:batch].size always 1
          tally(result, "nInserted", n)
        elsif op_type == :update
          n_upserted = 0
          if (upserted = response.fetch("upserted", nil)) # assignment
            upserted = [{"_id" => upserted}] if upserted.class != Array # OP_UPDATE non-array
            n_upserted = upserted.size
            concat(result, "upserted", merge_indexes(upserted, exchange))
          elsif (response["updatedExisting"] == false && n == 1)
            # workaround for DRIVERS-151 (non-ObjectID _id fields in pre-2.6 servers)
            op = exchange[:batch][0]
            missing_id = op[:u].fetch(:_id, op[:q][:_id]) # _id in update document takes precedence
            upserted = [ { "_id" => missing_id, "index" => 0 } ]
            n_upserted = n
            concat(result, "upserted", merge_indexes(upserted, exchange))
          end
          tally(result, "nUpserted", n_upserted) if n_upserted > 0
          tally(result, "nMatched", n - n_upserted)
          nil_tally(result, "nModified", response["nModified"])
        elsif op_type == :delete
          tally(result, "nRemoved", n)
        end
        result["n"] += n
        write_concern_error = nil
        errmsg = response["errmsg"] || response["err"] # top level
        if (writeErrors = response["writeErrors"] || response["errDetails"]) # assignment
          concat(result, "writeErrors", merge_indexes(writeErrors, exchange))
        elsif response["err"] == "timeout" # errmsg == "timed out waiting for slaves" # OP_*
          write_concern_error = {"errmsg" => errmsg, "code" => Mongo::ErrorCode::WRITE_CONCERN_FAILED,
                               "errInfo" => {"wtimeout" => response["wtimeout"]}} # OP_* does not have "code"
        elsif errmsg == "norepl" # OP_*
          write_concern_error = {"errmsg" => errmsg, "code" => Mongo::ErrorCode::WRITE_CONCERN_FAILED} # OP_* does not have "code"
        elsif errmsg # OP_INSERT, OP_UPDATE have "err"
          append(result, "writeErrors", merge_index({"errmsg" => errmsg, "code" => response["code"]}, exchange))
        end
        if response["writeConcernError"]
          write_concern_error = response["writeConcernError"]
        elsif (wnote = response["wnote"]) # assignment - OP_*
          write_concern_error = {"errmsg" => wnote, "code" => Mongo::ErrorCode::WRITE_CONCERN_FAILED} # OP_* does not have "code"
        elsif (jnote = response["jnote"]) # assignment - OP_*
          write_concern_error = {"errmsg" => jnote, "code" => Mongo::ErrorCode::BAD_VALUE} # OP_* does not have "code"
        end
        append(result, "writeConcernError", merge_index(write_concern_error, exchange)) if write_concern_error
      end
      result.delete("nModified") if result.has_key?("nModified") && !result["nModified"]
      result.merge!("ok" => [ok + result["n"], 1].min)
    end

    def initialize_copy(other)
      other.instance_variable_set(:@options, other.options.dup)
    end

    def op_args_set(op, value)
      @op_args[op] = value
      self
    end

    def op_push(op)
      raise MongoArgumentError, "non-nil query must be set via find" if op.first != :insert && !op.last[:q]
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
