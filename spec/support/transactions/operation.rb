# Copyright (C) 2014-2019 MongoDB, Inc.
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
  module Transactions
    class Operation

      # Map of operation names to method names.
      #
      # @since 2.6.0
      OPERATIONS = {
        'startTransaction' => :start_transaction,
        'abortTransaction' => :abort_transaction,
        'commitTransaction' => :commit_transaction,
        'withTransaction' => :with_transaction,
        'aggregate' => :aggregate,
        'deleteMany' => :delete_many,
        'deleteOne' => :delete_one,
        'insertMany' => :insert_many,
        'insertOne' => :insert_one,
        'replaceOne' => :replace_one,
        'updateMany' => :update_many,
        'updateOne' => :update_one,
        'findOneAndDelete' => :find_one_and_delete,
        'findOneAndReplace' => :find_one_and_replace,
        'findOneAndUpdate' => :find_one_and_update,
        'bulkWrite' => :bulk_write,
        'count' => :count,
        'distinct' => :distinct,
        'find' => :find,
        'runCommand' => :run_command,
      }.freeze

      # Map of operation options to method names.
      #
      # @since 2.6.0
      ARGUMENT_MAP = {
        array_filters: 'arrayFilters',
        batch_size: 'batchSize',
        collation: 'collation',
        read_preference: 'readPreference',
        document: 'document',
        field_name: 'fieldName',
        filter: 'filter',
        ordered: 'ordered',
        pipeline: 'pipeline',
        projection: 'projection',
        replacement: 'replacement',
        return_document: 'returnDocument',
        session: 'session',
        sort: 'sort',
        update: 'update',
        upsert: 'upsert'
      }.freeze

      # The operation name.
      #
      # @return [ String ] name The operation name.
      #
      # @since 2.6.0
      attr_reader :name

      # Instantiate the operation.
      #
      # @return [ Hash ] spec The operation spec.
      #
      # @since 2.6.0
      def initialize(spec, session0, session1, transaction_session=nil)
        @spec = spec
        @name = spec['name']
        @session0 = session0
        @session1 = session1
        @arguments = case spec['arguments'] && spec['arguments']['session']
                    when 'session0'
                      spec['arguments'].merge('session' => @session0)
                    when 'session1'
                      spec['arguments'].merge('session' => @session1)
                    else
                      args = spec['arguments'] || {}
                      if transaction_session
                        args = args.merge('session' => transaction_session)
                      end
                      args
                    end
      end

      attr_reader :arguments

      # Execute the operation.
      #
      # @example Execute the operation.
      #   operation.execute
      #
      # @param [ Collection ] collection The collection to execute
      #   the operation on.
      #
      # @return [ Result ] The result of executing the operation.
      #
      # @since 2.6.0
      def execute(collection)
        # Determine which object the operation method should be called on.
        obj = case object
              when 'session0'
                @session0
              when 'session1'
                @session1
              when 'database'
                collection.database
              else
                collection = collection.with(read: read_preference) if collection_read_preference
                collection = collection.with(read_concern: read_concern) if read_concern
                collection = collection.with(write: write_concern) if write_concern
                collection
              end

        if (op_name = OPERATIONS[name]) == :with_transaction
          args = [collection]
        else
          args = []
        end
        send(op_name, obj, *args)
      rescue Mongo::Error::OperationFailure => e
        err_doc = e.instance_variable_get(:@result).send(:first_document)

        {
          'errorCodeName' => err_doc['codeName'] || err_doc['writeConcernError']['codeName'],
          'errorContains' => e.message,
          'errorLabels' => e.labels,
          'exception' => e,
        }
      rescue Mongo::Error => e
        {
          'errorContains' => e.message,
          'errorLabels' => e.labels,
          'exception' => e,
        }
      end

      private

      def start_transaction(session)
        session.start_transaction(snakeize_hash(arguments['options'])) ; nil
      end

      def commit_transaction(session)
        session.commit_transaction ; nil
      end

      def abort_transaction(session)
        session.abort_transaction ; nil
      end

      def with_transaction(session, collection)
        unless callback = @spec['callback']
          raise ArgumentError, 'with_transaction requires a callback to be present'
        end

        if @spec['transactionOptions']
          options = snakeize_hash(@spec['transactionOptions'])
        end
        session.with_transaction(options) do
          callback['operations'].each do |op_spec|
            op = Operation.new(op_spec, @session0, @session1, session)
            rv = op.execute(collection)
            if rv && rv['exception']
              raise rv['exception']
            end
          end
        end
      end

      def run_command(database)
        # Convert the first key (i.e. the command name) to a symbol.
        cmd = command.dup
        command_name = cmd.first.first
        command_value = cmd.delete(command_name)
        cmd = { command_name.to_sym => command_value }.merge(cmd)

        opts = snakeize_hash(options)
        opts[:read] = opts.delete(:read_preference)
        database.command(cmd, opts).documents.first
      end

      def aggregate(collection)
        collection.aggregate(pipeline, options).to_a
      end

      def bulk_write(collection)
        result = collection.bulk_write(requests, options)
        return_doc = {}
        return_doc['deletedCount'] = result.deleted_count || 0
        return_doc['insertedIds'] = result.inserted_ids if result.inserted_ids
        return_doc['upsertedId'] = result.upserted_id if upsert
        return_doc['upsertedCount'] = result.upserted_count || 0
        return_doc['matchedCount'] = result.matched_count || 0
        return_doc['modifiedCount'] = result.modified_count || 0
        return_doc['upsertedIds'] = result.upserted_ids if result.upserted_ids
        return_doc
      end

      def count(collection)
        collection.count(filter, options).to_s
      end

      def delete_many(collection)
        result = collection.delete_many(filter, options)
        { 'deletedCount' => result.deleted_count }
      end

      def delete_one(collection)
        result = collection.delete_one(filter, options)
        { 'deletedCount' => result.deleted_count }
      end

      def distinct(collection)
        collection.distinct(field_name, filter, options)
      end

      def find(collection)
        opts = modifiers ? options.merge(modifiers: BSON::Document.new(modifiers)) : options
        collection.find(filter, opts).to_a
      end

      def insert_many(collection)
        result = collection.insert_many(documents, options)
        { 'insertedIds' => result.inserted_ids }
      end

      def insert_one(collection)
        result = collection.insert_one(document, options)
        { 'insertedId' => result.inserted_id }
      end

      def update_return_doc(result)
        return_doc = {}
        return_doc['upsertedId'] = result.upserted_id if upsert
        return_doc['upsertedCount'] = result.upserted_count
        return_doc['matchedCount'] = result.matched_count
        return_doc['modifiedCount'] = result.modified_count if result.modified_count
        return_doc
      end

      def replace_one(collection)
        result = collection.replace_one(filter, replacement, options)
        update_return_doc(result)
      end

      def update_many(collection)
        result = collection.update_many(filter, update, options)
        update_return_doc(result)
      end

      def update_one(collection)
        result = collection.update_one(filter, update, options)
        update_return_doc(result)
      end

      def find_one_and_delete(collection)
        collection.find_one_and_delete(filter, options)
      end

      def find_one_and_replace(collection)
        collection.find_one_and_replace(filter, replacement, options)
      end

      def find_one_and_update(collection)
        collection.find_one_and_update(filter, update, options)
      end

      def object
        @spec['object']
      end

      def options
        ARGUMENT_MAP.reduce({}) do |opts, (key, value)|
          arguments.key?(value) ? opts.merge!(key => send(key)) : opts
        end
      end

      def collation
        arguments['collation']
      end

      def command
        arguments['command']
      end

      def replacement
        arguments['replacement']
      end

      def sort
        arguments['sort']
      end

      def projection
        arguments['projection']
      end

      def documents
        arguments['documents']
      end

      def document
        arguments['document']
      end

      def ordered
        arguments['ordered']
      end

      def field_name
        arguments['fieldName']
      end

      def filter
        arguments['filter']
      end

      def pipeline
        arguments['pipeline']
      end

      def array_filters
        arguments['arrayFilters']
      end

      def batch_size
        arguments['batchSize']
      end

      def session
        arguments['session']
      end

      def requests
        arguments['requests'].map do |request|
          case request.keys.first
          when 'insertOne' then
            { insert_one: request['insertOne']['document'] }
          when 'updateOne' then
            update = request['updateOne']
            { update_one: { filter: update['filter'], update: update['update'] } }
          when 'name' then
            bulk_request(request)
          end
        end
      end

      def bulk_request(request)
        op_name = OPERATIONS[request['name']]
        op = { op_name => {} }

        op[op_name][:filter] = request['arguments']['filter'] if request['arguments']['filter']
        op[op_name][:update] = request['arguments']['update'] if request['arguments']['update']
        op[op_name][:upsert] = request['arguments']['upsert'] if request['arguments']['upsert']
        op[op_name][:replacement] = request['arguments']['replacement'] if request['arguments']['replacement']
        op[op_name][:array_filters] =  request['arguments']['arrayFilters'] if request['arguments']['arrayFilters']
        op[op_name] = request['arguments']['document'] if request['arguments']['document']
        op
      end

      def upsert
        arguments['upsert']
      end

      def return_document
        case arguments['returnDocument']
        when 'Before'
          :before
        when 'After'
          :after
        end
      end

      def update
        arguments['update']
      end

      def modifiers
        arguments['modifiers']
      end

      def read_concern
        snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['readConcern'])
      end

      def write_concern
        snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['writeConcern'])
      end

      def read_preference
        snakeize_hash(arguments['readPreference'])
      end

      def collection_read_preference
        snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['readPreference'])
      end
    end
  end
end
