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
      def initialize(spec)
        @spec = IceNine.deep_freeze(spec)
        @name = spec['name']
        @arguments = spec['arguments'] || {}
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
      def execute(collection, session0, session1, active_session=nil)
        # Determine which object the operation method should be called on.
        obj = case object
        when 'session0'
          session0
        when 'session1'
          session1
        when 'database'
          collection.database
        else
          if rp = collection_read_preference
            collection = collection.with(read: rp)
          end
          collection = collection.with(read_concern: read_concern) if read_concern
          collection = collection.with(write: write_concern) if write_concern
          collection
        end

        session = case arguments && arguments['session']
        when 'session0'
          session0
        when 'session1'
          session1
        else
          if active_session
            active_session
          else
            nil
          end
        end

        context = Context.new(
          session0,
          session1,
          session)

        op_name = Utils.underscore(name).to_sym
        if op_name == :with_transaction
          args = [collection]
        else
          args = []
        end
        if op_name.nil?
          raise "Unknown operation #{name}"
        end
        send(op_name, obj, context, *args)
      rescue Mongo::Error::OperationFailure => e
        err_doc = e.instance_variable_get(:@result).send(:first_document)
        error_code_name = err_doc['codeName'] || err_doc['writeConcernError'] && err_doc['writeConcernError']['codeName']
        if error_code_name.nil?
          # Sometimes the server does not return the error code name,
          # but does return the error code (or we can parse the error code
          # out of the message).
          # https://jira.mongodb.org/browse/SERVER-39706
          if e.code == 11000
            error_code_name = 'DuplicateKey'
          else
            warn "Error without error code name: #{e.code}"
          end
        end

        {
          'errorCodeName' => error_code_name,
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

      def start_transaction(session, context)
        session.start_transaction(Utils.snakeize_hash(arguments['options'])) ; nil
      end

      def commit_transaction(session, context)
        session.commit_transaction ; nil
      end

      def abort_transaction(session, context)
        session.abort_transaction ; nil
      end

      def with_transaction(session, context, collection)
        unless callback = arguments['callback']
          raise ArgumentError, 'with_transaction requires a callback to be present'
        end

        if arguments['options']
          options = Utils.snakeize_hash(arguments['options'])
        else
          options = nil
        end
        session.with_transaction(options) do
          callback['operations'].each do |op_spec|
            op = Operation.new(op_spec)
            rv = op.execute(collection, context.session0, context.session1, session)
            if rv && rv['exception']
              raise rv['exception']
            end
          end
        end
      end

      def run_command(database, context)
        # Convert the first key (i.e. the command name) to a symbol.
        cmd = arguments['command'].dup
        command_name = cmd.first.first
        command_value = cmd.delete(command_name)
        cmd = { command_name.to_sym => command_value }.merge(cmd)

        opts = Utils.snakeize_hash(context.transform_arguments(options))
        opts[:read] = opts.delete(:read_preference)
        database.command(cmd, opts).documents.first
      end

      def aggregate(collection, context)
        collection.aggregate(arguments['pipeline'], context.transform_arguments(options)).to_a
      end

      def bulk_write(collection, context)
        result = collection.bulk_write(requests, context.transform_arguments(options))
        return_doc = {}
        return_doc['deletedCount'] = result.deleted_count || 0
        return_doc['insertedIds'] = result.inserted_ids if result.inserted_ids
        return_doc['upsertedId'] = result.upserted_id if arguments['upsert']
        return_doc['upsertedCount'] = result.upserted_count || 0
        return_doc['matchedCount'] = result.matched_count || 0
        return_doc['modifiedCount'] = result.modified_count || 0
        return_doc['upsertedIds'] = result.upserted_ids if result.upserted_ids
        return_doc
      end

      def count(collection, context)
        collection.count(arguments['filter'], context.transform_arguments(options)).to_s
      end

      def count_documents(collection, context)
        collection.count_documents(arguments['filter'], context.transform_arguments(options))
      end

      def delete_many(collection, context)
        result = collection.delete_many(arguments['filter'], context.transform_arguments(options))
        { 'deletedCount' => result.deleted_count }
      end

      def delete_one(collection, context)
        result = collection.delete_one(arguments['filter'], context.transform_arguments(options))
        { 'deletedCount' => result.deleted_count }
      end

      def distinct(collection, context)
        collection.distinct(arguments['fieldName'], arguments['filter'], context.transform_arguments(options))
      end

      def find(collection, context)
        opts = context.transform_arguments(options)
        if arguments['modifiers']
          opts = opts.merge(modifiers: BSON::Document.new(arguments['modifiers']))
        end
        collection.find(arguments['filter'], opts).to_a
      end

      def insert_many(collection, context)
        result = collection.insert_many(arguments['documents'], context.transform_arguments(options))
        { 'insertedIds' => result.inserted_ids }
      end

      def insert_one(collection, context)
        result = collection.insert_one(arguments['document'], context.transform_arguments(options))
        { 'insertedId' => result.inserted_id }
      end

      def update_return_doc(result)
        return_doc = {}
        return_doc['upsertedId'] = result.upserted_id if arguments['upsert']
        return_doc['upsertedCount'] = result.upserted_count
        return_doc['matchedCount'] = result.matched_count
        return_doc['modifiedCount'] = result.modified_count if result.modified_count
        return_doc
      end

      def replace_one(collection, context)
        result = collection.replace_one(arguments['filter'], arguments['replacement'], context.transform_arguments(options))
        update_return_doc(result)
      end

      def update_many(collection, context)
        result = collection.update_many(arguments['filter'], arguments['update'], context.transform_arguments(options))
        update_return_doc(result)
      end

      def update_one(collection, context)
        result = collection.update_one(arguments['filter'], arguments['update'], context.transform_arguments(options))
        update_return_doc(result)
      end

      def find_one_and_delete(collection, context)
        collection.find_one_and_delete(arguments['filter'], context.transform_arguments(options))
      end

      def find_one_and_replace(collection, context)
        collection.find_one_and_replace(arguments['filter'], arguments['replacement'], context.transform_arguments(options))
      end

      def find_one_and_update(collection, context)
        collection.find_one_and_update(arguments['filter'], arguments['update'], context.transform_arguments(options))
      end

      def object
        @spec['object']
      end

      def options
        out = {}
        arguments.each do |spec_k, v|
          ruby_k = Utils.underscore(spec_k).to_sym

          if respond_to?("transform_#{ruby_k}", true)
            v = send("transform_#{ruby_k}", v)
          end

          out[ruby_k] = v
        end
        out
      end

      def requests
        arguments['requests'].map do |request|
          case request.keys.first
          when 'insertOne' then
            { insert_one: request['insertOne']['document'] }
          when 'updateOne' then
            update = request['updateOne']
            { update_one: { filter: arguments['update']['filter'], update: arguments['update']['update'] } }
          when 'name' then
            bulk_request(request)
          end
        end
      end

      def bulk_request(request)
        op_name = Utils.underscore(request['name']).to_sym
        op = { op_name => {} }

        op[op_name][:filter] = request['arguments']['filter'] if request['arguments']['filter']
        op[op_name][:update] = request['arguments']['update'] if request['arguments']['update']
        op[op_name][:upsert] = request['arguments']['upsert'] if request['arguments']['upsert']
        op[op_name][:replacement] = request['arguments']['replacement'] if request['arguments']['replacement']
        op[op_name][:array_filters] =  request['arguments']['arrayFilters'] if request['arguments']['arrayFilters']
        op[op_name] = request['arguments']['document'] if request['arguments']['document']
        op
      end

      def transform_return_document(v)
        Utils.underscore(v).to_sym
      end

      def read_concern
        Utils.snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['readConcern'])
      end

      def write_concern
        Utils.snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['writeConcern'])
      end

      def transform_read_preference(v)
        Utils.snakeize_hash(v)
      end

      def collection_read_preference
        Utils.snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['readPreference'])
      end
    end
  end
end
