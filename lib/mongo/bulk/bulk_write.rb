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

  # This semi-public class handles the logic for executing a batch of
  # operations.
  #
  # @since 2.0.0
  class BulkWrite

    # @return [ Mongo::Collection ] The collection on which this bulk write
    #   operation will be executed.
    attr_reader :collection

    # @return [ Array<Hash> ] operations The list of operations.
    attr_reader :operations

    # @return [ Hash ] options The options.
    attr_reader :options

    # @return [ Acknowledged, Unacknowledged ] write_concern The write concern.
    attr_reader :write_concern

    # Initialize the BulkWrite object.
    #
    # @example Create an ordered bulk write object.
    #   BulkWrite.new([
    #       { insert_one: { x: 1 }},
    #       { update_one: [{ x: 1 }, { '$set' => { x: 2 }}]}
    #     ],
    #     { ordered: true },
    #     collection
    #   )
    #
    # @example Create an unordered bulk write object.
    #   BulkWrite.new([
    #       { insert_one: { x: 1 }},
    #       { update_one: [{ x: 1 }, { '$set' => { x: 2 }}]}
    #     ],
    #     { ordered: false },
    #     collection
    #   )
    #
    # @param [ Array<Hash> ] operations The operations to execute.
    # @param [ Hash ] options The options for executing the operations.
    # @param [ Collection ] collection The collection on which the
    #   operations will be executed.
    #
    # @option options [ String ] :ordered Whether the operations should
    #   be executed in order.
    # @option options [ Hash ] :write_concern The write concern to use when
    #   executing the operations.
    #
    # @since 2.0.0
    def initialize(operations, options, collection)
      @operations = operations
      @options = options
      @collection = collection
      if options[:write_concern]
        @write_concern = WriteConcern.get(options[:write_concern])
      else
        @write_concern = collection.write_concern
      end
    end

    # Is the bulk write operation ordered?
    #
    # @example Is the bulk write operation ordered?
    #   bulk_write.ordered?
    #
    # @return [ true, false ] If the bulk write is ordered.
    #
    # @since 2.0.0
    def ordered?
      @ordered ||= !!options[:ordered]
    end

    # Execute the bulk write.
    #
    # @example execute the bulk write operations.
    #   bulk.execute
    #
    # @return [ Hash ] The result of doing the bulk write.
    def execute
      raise Error::EmptyBatch.new if operations.empty?

      @index = -1
      @ops = []

      operations.each do |operation|
        operation.each do |name, document|
          if respond_to?(name, true)
            send(name, document)
          else
            raise Error::InvalidBulkOperation.new(name)
          end
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
          # @todo: No test for max message size.
        rescue Error::MaxBSONSize, Error::MaxMessageSize => ex
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
      raise Error::InvalidDocument.new unless valid_doc?(doc)
      spec = { documents: [ doc ],
               db_name: db_name,
               coll_name: collection.name,
               ordered: ordered?,
               write_concern: write_concern }

      push_op(Mongo::Operation::Write::BulkInsert, spec)
    end

    def delete_one(selector)
      raise Error::InvalidDocument.new unless valid_doc?(selector)
      spec = { deletes: [{ q: selector, limit: 1 }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: ordered?,
               write_concern: write_concern }

      push_op(Mongo::Operation::Write::BulkDelete, spec)
    end

    def delete_many(selector)
      raise Error::InvalidDocument.new unless valid_doc?(selector)
      spec = { deletes: [{ q: selector, limit: 0 }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: ordered?,
               write_concern: write_concern }

      push_op(Mongo::Operation::Write::BulkDelete, spec)
    end

    def replace_one(docs)
      selector = docs[0]
      replacement = docs[1]
      upsert = (docs[2] || {})[:upsert]
      raise ArgumentError unless selector && replacement
      raise Error::InvalidReplacementDocument.new unless replacement_doc?(replacement)
      upsert = !!upsert
      spec = { updates: [{ q: selector,
                           u: replacement,
                           multi: false,
                           upsert: upsert }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: ordered?,
               write_concern: write_concern }

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
      raise Error::InvalidUpdateDocument.new unless update_doc?(update)
      upsert = !!upsert
      spec = { updates: [{ q: selector,
                           u: update,
                           multi: multi,
                           upsert: upsert }],
               db_name:   db_name,
               coll_name: collection.name,
               ordered: ordered?,
               write_concern: write_concern }

      push_op(Mongo::Operation::Write::BulkUpdate, spec)
    end

    def push_op(op_class, spec)
      spec.merge!(indexes: [ increment_index ])
      @ops << op_class.send(:new, spec)
    end

    def merge_ops
      if ordered?
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

    def stop_executing?(result)
      result && ordered? && !result.successful?
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
        raise Error::BulkWriteFailure.new(response)
      end
      response
    end

    def db_name
      collection.database.name
    end
  end
end
