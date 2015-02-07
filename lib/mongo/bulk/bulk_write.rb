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

module Mongo
  class BulkWrite

    # This semi-public class handles the logic for executing a batch of
    # operations

    # @return [ Mongo::Collection ] The collection on which this bulk write
    #   operation will be executed.
    attr_reader :collection

    # Initialize the BulkWrite object.
    #
    # @example Create an ordered bulk write object.
    #   BulkWrite.new([ { insert_one: { x: 1 } },
    #                   { update_one: [ { x: 1 }, { '$set' => { x: 2 } } ] }
    #                 ],
    #                 { ordered: true }, collection)
    #
    # @example Create an unordered bulk write object.
    #   BulkWrite.new([ { insert_one: { x: 1 } },
    #                   { update_one: [ { x: 2 }, { '$set' => { x: 3 } } ] }
    #                 ],
    #                 { ordered: false }, collection)
    #
    # @param [ Array ] operations The operations to execute.
    # @param [ Hash ] options The options for executing the operations.
    # @param [ Collection ] collection The collection on which the
    #   operations will be executed.
    #
    # @option options [ String ] :ordered Whether the operations should
    #   be executed in order.
    # @option options [ Hash ] :write_concern The write concern to use when
    #   executing the operations.
    def initialize(operations, options, collection)
      @operations = operations
      @ordered = !!options[:ordered]
      @collection = collection
      if options[:write_concern]
        @write_concern = WriteConcern.get(options[:write_concern])
      else
        @write_concern = collection.write_concern
      end
    end

    # Execute the bulk write.
    #
    # @example execute the bulk write operations.
    #   bulk.execute
    #
    # @return [ Hash ] The result of doing the bulk write.
    def execute
      # @todo: No test for this case.
      raise Error::EmptyBatch.new if @operations.empty?

      @index = -1
      @ops = []

      @operations.each do |op|
        op_name = op.keys.first
        begin
          send(op_name, op[op_name])
        rescue NoMethodError
          raise InvalidOpType.new(op_name)
        end
      end

      ops = merge_ops

      replies = []
      until ops.empty?
        op = ops.shift

        until op.valid_batch_size?(collection.next_primary.context.max_write_batch_size)
          ops = op.batch(2) + ops
          op = ops.shift
        end

        begin
          replies << op.execute(collection.next_primary.context)
        rescue Protocol::Serializers::Document::InvalidBSONSize,
               Server::Connection::InvalidMessageSize => ex
          raise ex unless op.batchable?
          ops = op.batch(2) + ops
        end
        return make_response!(replies) if stop_executing?(replies.last)
      end
      make_response!(replies) if op.write_concern.get_last_error
    end

    private

    def increment_index
      @index += 1
    end

    def valid_doc?(doc)
      doc.respond_to?(:keys)
    end

    def update_doc?(doc)
      !doc.empty? &&
        doc.respond_to?(:keys) &&
        doc.keys.first.to_s =~ /^\$/
    end

    def replacement_doc?(doc)
      doc.respond_to?(:keys) && doc.keys.all?{|key| key !~ /^\$/}
    end

    def insert_one(doc)
      raise InvalidDoc.new unless valid_doc?(doc)
      spec = { documents: [ doc ],
               db_name: db_name,
               coll_name: collection.name,
               ordered: @ordered,
               write_concern: @write_concern }

      push_op(Mongo::Operation::Write::BulkInsert, spec)
    end

    def delete_one(selector)
      raise InvalidDoc.new unless valid_doc?(selector)
      spec = { deletes:   [{ q: selector,
                             limit: 1 }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: @ordered,
               write_concern: @write_concern }

      push_op(Mongo::Operation::Write::BulkDelete, spec)
    end

    def delete_many(selector)
      raise InvalidDoc.new unless valid_doc?(selector)
      spec = { deletes:   [{ q: selector,
                             limit: 0 }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: @ordered,
               write_concern: @write_concern }

      push_op(Mongo::Operation::Write::BulkDelete, spec)
    end

    def replace_one(docs)
      selector = docs[0]
      replacement = docs[1]
      upsert = (docs[2] || {})[:upsert]
      raise ArgumentError unless selector && replacement
      raise InvalidReplacementDoc.new unless replacement_doc?(replacement)
      upsert = !!upsert
      spec = { updates:   [{ q: selector,
                             u: replacement,
                             multi: false,
                             upsert: upsert }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: @ordered,
               write_concern: @write_concern }

      push_op(Mongo::Operation::Write::BulkUpdate, spec)
    end

    def update_one(docs)
      upsert = (docs[2] || {})[:upsert]
      update_one_or_many(docs[0], docs[1], upsert, false)
    end

    def update_many(docs)
      upsert = (docs[2] || {})[:upsert]
      update_one_or_many(docs[0], docs[1], upsert, true)
    end

    def update_one_or_many(selector, update, upsert, multi)
      raise ArgumentError unless selector && update
      raise InvalidUpdateDoc.new unless update_doc?(update)
      upsert = !!upsert
      spec = { updates:   [{ q: selector,
                             u: update,
                             multi: multi,
                             upsert: upsert }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: @ordered,
               write_concern: @write_concern }

      push_op(Mongo::Operation::Write::BulkUpdate, spec)
    end

    def push_op(op_class, spec)
      spec.merge!(indexes: [ increment_index ])
      @ops << op_class.send(:new, spec)
    end

    def merge_ops
      if @ordered
        merge_consecutive_ops(@ops)
      else
        merge_ops_by_type(@ops)
      end
    end

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

    def stop_executing?(reply)
      reply && @ordered && reply.write_failure?
    end

    def make_response!(results)
      response = results.reduce({}) do |response, result|
        write_errors = result.aggregate_write_errors
        write_concern_errors = result.aggregate_write_concern_errors
        response.tap do |r|
          r['nInserted'] = ( r['nInserted'] || 0 ) + result.n_inserted if result.respond_to?(:n_inserted)
          r['nMatched'] = ( r['nMatched'] || 0 ) + result.n_matched if result.respond_to?(:n_matched)
          r['nModified'] = ( r['nModified'] || 0 ) + result.n_modified if result.respond_to?(:n_modified) && result.n_modified
          r['nUpserted'] = ( r['nUpserted'] || 0 ) + result.n_upserted if result.respond_to?(:n_upserted)
          r['nRemoved'] = ( r['nRemoved'] || 0 ) + result.n_removed if result.respond_to?(:n_removed)
          r['writeErrors'] = ( r['writeErrors'] || [] ) + write_errors if write_errors
          r['writeConcernErrors'] = ( r['writeConcernErrors'] || [] ) + write_concern_errors if write_concern_errors
        end
      end

      if response['writeErrors'] || response['writeConcernErrors']
        response.merge!('errmsg' => 'batch item errors occurred')
        raise Mongo::BulkWrite::BulkWriteError.new(response)
      end
      response
    end

    def db_name
      collection.database.name
    end

    # Exception raised if there are write errors upon executing the bulk
    # operation.
    #
    # @since 2.0.0
    class BulkWriteError < OperationError

      attr_reader :result

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Bulk::BulkWrite::BulkWriteError.new(response)
      #
      # @params [ Hash ] result A processed response from the server
      #   reporting results of the operation.
      #
      # @since 2.0.0
      def initialize(result)
        @result = result
      end
    end

    # Exception raised if the object is not a valid document.
    #
    # @since 2.0.0
    class InvalidDoc < Error::DriverError

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::BulkWrite::InvalidDoc.new
      #
      # @since 2.0.0
      def initialize
        super("Invalid document provided.")
      end
    end

    # Exception raised if the object is not a valid update document.
    #
    # @since 2.0.0
    class InvalidUpdateDoc < Error::DriverError

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::BulkWrite::InvalidUpdateDoc.new
      #
      # @since 2.0.0
      def initialize
        super("Invalid update document provided.")
      end
    end

    # Exception raised if the object is not a valid replacement document.
    #
    # @since 2.0.0
    class InvalidReplacementDoc < Error::DriverError

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::BulkWrite::InvalidReplacementDoc.new
      #
      # @since 2.0.0
      def initialize
        super("Invalid replacement document provided.")
      end
    end

    # Exception raised if an non-existent operation type is used.
    #
    # @since 2.0.0
    class InvalidOpType < Error::DriverError

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::BulkWrite::InvalidDoc.new
      #
      # @since 2.0.0
      def initialize(op_name)
        super("Invalid operation type: #{op_name}")
      end
    end
  end
end
