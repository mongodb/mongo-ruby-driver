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
  module CRUD

    class Operation

      # Instantiate the operation.
      #
      # @param [ Hash ] spec The operation specification.
      # @param [ Hash ] outcome_spec The outcome specification.
      #   If not provided, outcome is taken out of operation specification.
      #
      # @since 2.0.0
      def initialize(crud_test, spec, outcome_spec = nil)
        @crud_test = crud_test
        @spec = IceNine.deep_freeze(spec)
        @name = spec['name']
        @arguments = spec['arguments'] || {}
        @outcome = Outcome.new(outcome_spec || spec)
      end

      # The operation name.
      #
      # @return [ String ] name The operation name.
      #
      # @since 2.0.0
      attr_reader :name

      attr_reader :arguments

      attr_reader :outcome

      def object
        @spec['object'] || 'collection'
      end

      # Which collection to verify results from.
      # Returns the collection name specified on the operation, or
      # the collection name for the entire spec file.
      def verify_collection_name
        if outcome && outcome.collection_name
          outcome.collection_name
        else
          @spec['collection_name'] || 'crud_spec_test'
        end
      end

      # Whether the operation is expected to have results.
      #
      # @example Whether the operation is expected to have results.
      #   operation.has_results?
      #
      # @return [ true, false ] If the operation is expected to have results.
      #
      # @since 2.0.0
      def has_results?
        !(name == 'aggregate' &&
            pipeline.find {|op| op.keys.include?('$out') })
      end

      # Execute the operation.
      #
      # @example Execute the operation.
      #   operation.execute
      #
      # @param [ Collection ] collection The collection to execute the operation on.
      #
      # @return [ Result, Array<Hash> ] The result of executing the operation.
      #
      # @since 2.0.0
      def execute(target)
        op_name = Utils.underscore(name)
        if target.is_a?(Mongo::Database)
          op_name = "db_#{op_name}"
        elsif target.is_a?(Mongo::Client)
          op_name= "client_#{op_name}"
        end
        send(op_name, target, Context.new)
      end

      def collection_options
        Utils.convert_operation_options(@spec['collectionOptions'])
      end

      private

      # read operations

      def aggregate(collection, context)
        collection.aggregate(arguments['pipeline'], context.transform_arguments(options)).to_a
      end

      def count(collection, context)
        collection.count(arguments['filter'], context.transform_arguments(options))
      end

      def count_documents(collection, context)
        collection.count_documents(arguments['filter'], context.transform_arguments(options))
      end

      def distinct(collection, context)
        collection.distinct(arguments['fieldName'], arguments['filter'], context.transform_arguments(options))
      end

      def estimated_document_count(collection, context)
        collection.estimated_document_count(context.transform_arguments(options))
      end

      def find(collection, context)
        opts = context.transform_arguments(options)
        if arguments['modifiers']
          opts = opts.merge(modifiers: BSON::Document.new(arguments['modifiers']))
        end
        if read_preference
          collection = collection.with(read: read_preference)
        end
        collection.find(arguments['filter'], opts).to_a
      end

      def find_one(collection, context)
        find(collection, context).first
      end

      def client_list_databases(client, context)
        client.list_databases
      end

      def client_list_database_names(client, context)
        client.list_databases({}, true)
      end

      def client_list_database_objects(client, context)
        client.list_mongo_databases
      end

      def db_list_collections(database, context)
        database.list_collections
      end

      def db_list_collection_names(database, context)
        database.collection_names
      end

      def db_list_collection_objects(database, context)
        database.collections
      end

      def list_indexes(collection, context)
        collection.indexes.to_a
      end

      def watch(collection, context)
        collection.watch
      end

      def db_watch(database, context)
        database.watch
      end

      def client_watch(client, context)
        client.watch
      end

      def download(fs_bucket, context)
        stream = fs_bucket.open_download_stream(BSON::ObjectId.from_string(arguments['id']['$oid']))
        stream.read
      end

      def download_by_name(fs_bucket, context)
        stream = fs_bucket.open_download_stream_by_name(arguments['filename'])
        stream.read
      end

      def map_reduce(collection, context)
        view = Mongo::Collection::View.new(collection)
        mr = Mongo::Collection::View::MapReduce.new(view, arguments['map']['$code'], arguments['reduce']['$code'])
        mr.to_a
      end

      # write operations

      def bulk_write(collection, context)
        result = collection.bulk_write(requests, context.transform_arguments(options))
        return_doc = {}
        return_doc['deletedCount'] = result.deleted_count || 0
        return_doc['insertedIds'] = result.inserted_ids if result.inserted_ids
        return_doc['insertedCount'] = result.inserted_count || 0
        return_doc['upsertedId'] = result.upserted_id if arguments['upsert']
        return_doc['upsertedIds'] = result.upserted_ids if result.upserted_ids
        return_doc['upsertedCount'] = result.upserted_count || 0
        return_doc['matchedCount'] = result.matched_count || 0
        return_doc['modifiedCount'] = result.modified_count || 0
        return_doc
      end

      def delete_many(collection, context)
        result = collection.delete_many(arguments['filter'], context.transform_arguments(options))
        { 'deletedCount' => result.deleted_count }
      end

      def delete_one(collection, context)
        result = collection.delete_one(arguments['filter'], context.transform_arguments(options))
        { 'deletedCount' => result.deleted_count }
      end

      def insert_many(collection, context)
        result = collection.insert_many(arguments['documents'], context.transform_arguments(options))
        { 'insertedIds' => result.inserted_ids }
      end

      def insert_one(collection, context)
        result = collection.insert_one(arguments['document'], context.transform_arguments(options))
        { 'insertedId' => result.inserted_id }
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

      # options & arguments

      def options
        out = {}
        # Most tests have an "arguments" key which is a hash of options to
        # be provided to the operation. The command monitoring unacknowledged
        # bulk write test is an exception in that it has an "options" key
        # with the options.
        arguments.merge(arguments['options'] || {}).each do |spec_k, v|
          ruby_k = Utils.underscore(spec_k).to_sym

          if v.is_a?(Hash) && v['$numberLong']
            v = v['$numberLong'].to_i
          end

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
            { update_one: { filter: update['filter'], update: update['update'] } }
          when 'name' then
            bulk_request(request)
          end
        end
      end

      def bulk_request(request)
        op_name = Utils.underscore(request['name'])
        args = Utils.shallow_snakeize_hash(request['arguments'])
        if args[:document]
          unless args.keys == [:document]
            raise "If :document is given, it must be the only key"
          end
          args = args[:document]
        end
        { op_name => args }
      end

      def upsert
        arguments['upsert']
      end

      def transform_return_document(v)
        Utils.underscore(v).to_sym
      end

      def update
        arguments['update']
      end

      def transform_read_preference(v)
        Utils.snakeize_hash(v)
      end

      def read_preference
        transform_read_preference(@spec['read_preference'])
      end

      def update_return_doc(result)
        return_doc = {}
        return_doc['upsertedId'] = result.upserted_id if arguments['upsert']
        return_doc['upsertedCount'] = result.upserted_count
        return_doc['matchedCount'] = result.matched_count
        return_doc['modifiedCount'] = result.modified_count if result.modified_count
        return_doc
      end
    end
  end
end
