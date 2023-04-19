# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
        if spec['arguments']
          @arguments = BSON::ExtJSON.parse_obj(spec['arguments'], mode: :bson)
        else
          @arguments = {}
        end
        @outcome = Outcome.new(outcome_spec || spec)
      end

      attr_reader :spec

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
        op_name = ::Utils.underscore(name)
        if target.is_a?(Mongo::Database)
          op_name = "db_#{op_name}"
        elsif target.is_a?(Mongo::Client)
          op_name = "client_#{op_name}"
        end
        send(op_name, target, Context.new)
      end

      def database_options
        if opts = @spec['databaseOptions']
          ::Utils.convert_operation_options(opts)
        else
          nil
        end
      end

      def collection_options
        ::Utils.convert_operation_options(@spec['collectionOptions'])
      end

      private

      # read operations

      def aggregate(collection, context)
        collection.aggregate(arguments['pipeline'], transformed_options(context)).to_a
      end

      def db_aggregate(database, context)
        database.aggregate(arguments['pipeline'], transformed_options(context)).to_a
      end

      def count(collection, context)
        collection.count(arguments['filter'], transformed_options(context))
      end

      def count_documents(collection, context)
        collection.count_documents(arguments['filter'], transformed_options(context))
      end

      def distinct(collection, context)
        collection.distinct(arguments['fieldName'], arguments['filter'], transformed_options(context))
      end

      def estimated_document_count(collection, context)
        collection.estimated_document_count(transformed_options(context))
      end

      def find(collection, context)
        opts = transformed_options(context)
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
        stream = fs_bucket.open_download_stream(arguments['id'])
        stream.read
      end

      def download_by_name(fs_bucket, context)
        stream = fs_bucket.open_download_stream_by_name(arguments['filename'])
        stream.read
      end

      def map_reduce(collection, context)
        view = Mongo::Collection::View.new(collection)
        mr = Mongo::Collection::View::MapReduce.new(view, arguments['map'].javascript, arguments['reduce'].javascript)
        mr.to_a
      end

      # write operations

      def bulk_write(collection, context)
        result = collection.bulk_write(requests, transformed_options(context))
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
        result = collection.delete_many(arguments['filter'], transformed_options(context))
        { 'deletedCount' => result.deleted_count }
      end

      def delete_one(collection, context)
        result = collection.delete_one(arguments['filter'], transformed_options(context))
        { 'deletedCount' => result.deleted_count }
      end

      def insert_many(collection, context)
        result = collection.insert_many(arguments['documents'], transformed_options(context))
        { 'insertedIds' => result.inserted_ids }
      end

      def insert_one(collection, context)
        result = collection.insert_one(arguments['document'], transformed_options(context))
        { 'insertedId' => result.inserted_id }
      end

      def replace_one(collection, context)
        result = collection.replace_one(arguments['filter'], arguments['replacement'], transformed_options(context))
        update_return_doc(result)
      end

      def update_many(collection, context)
        result = collection.update_many(arguments['filter'], arguments['update'], transformed_options(context))
        update_return_doc(result)
      end

      def update_one(collection, context)
        result = collection.update_one(arguments['filter'], arguments['update'], transformed_options(context))
        update_return_doc(result)
      end

      def find_one_and_delete(collection, context)
        collection.find_one_and_delete(arguments['filter'], transformed_options(context))
      end

      def find_one_and_replace(collection, context)
        collection.find_one_and_replace(arguments['filter'], arguments['replacement'], transformed_options(context))
      end

      def find_one_and_update(collection, context)
        collection.find_one_and_update(arguments['filter'], arguments['update'], transformed_options(context))
      end

      # ddl

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

      def create_collection(database, context)
        opts = transformed_options(context)
        database[arguments.fetch('collection')]
          .create(
            {
              session: opts[:session],
              encrypted_fields: opts[:encrypted_fields],
              validator: opts[:validator],
            }.compact
          )
      end

      def rename(collection, context)
        collection.client.use(:admin).command({
          renameCollection: "#{collection.database.name}.#{collection.name}",
          to: "#{collection.database.name}.#{arguments['to']}"
        })
      end

      def drop(collection, context)
        opts = transformed_options(context)
        collection.drop(encrypted_fields: opts[:encrypted_fields])
      end

      def drop_collection(database, context)
        opts = transformed_options(context)
        database[arguments.fetch('collection')].drop(encrypted_fields: opts[:encrypted_fields])
      end

      def create_index(collection, context)
        # The Ruby driver method uses `key` while the createIndexes server
        # command and the test specifiecation use 'keys`.
        opts = BSON::Document.new(options)
        if opts.key?(:keys)
          opts[:key] = opts.delete(:keys)
        end
        session = opts.delete(:session)
        collection.indexes(session: session && context.send(session)).create_many([opts])
      end

      def drop_index(collection, context)
        unless options.keys == %i(name)
          raise "Only name is allowed when dropping the index"
        end
        name = options[:name]
        collection.indexes.drop_one(name)
      end

      def list_indexes(collection, context)
        collection.indexes.to_a
      end

      # special

      def assert_collection_exists(client, context)
        c = client.use(dn = arguments.fetch('database'))
        unless c.database.collection_names.include?(cn = arguments.fetch('collection'))
          raise "Collection #{cn} does not exist in database #{dn}, but must"
        end
      end

      def assert_collection_not_exists(client, context)
        c = client.use(dn = arguments.fetch('database'))
        if c.database.collection_names.include?(cn = arguments.fetch('collection'))
          raise "Collection #{cn} exists in database #{dn}, but must not"
        end
      end

      def assert_index_exists(client, context)
        c = client.use(dn = arguments.fetch('database'))
        coll = c[cn = arguments.fetch('collection')]
        unless coll.indexes.map { |doc| doc['name'] }.include?(ixn = arguments.fetch('index'))
          raise "Index #{ixn} does not exist in collection #{cn} in database #{dn}, but must"
        end
      end

      def assert_index_not_exists(client, context)
        c = client.use(dn = arguments.fetch('database'))
        coll = c[cn = arguments.fetch('collection')]
        begin
          if coll.indexes.map { |doc| doc['name'] }.include?(ixn = arguments.fetch('index'))
            raise "Index #{ixn} exists in collection #{cn} in database #{dn}, but must not"
          end
        rescue Mongo::Error::OperationFailure => e
          if e.to_s =~ /ns does not exist/
            # Success.
          else
            raise
          end
        end
      end

      def configure_fail_point(client, context)
        fp = arguments.fetch('failPoint')
        $disable_fail_points ||= []
        $disable_fail_points << [
          fp,
          ClusterConfig.instance.primary_address,
        ]
        client.use('admin').database.command(fp)
      end

      # options & arguments

      def options
        out = {}
        # Most tests have an "arguments" key which is a hash of options to
        # be provided to the operation. The command monitoring unacknowledged
        # bulk write test is an exception in that it has an "options" key
        # with the options.
        arguments.merge(arguments['options'] || {}).each do |spec_k, v|
          ruby_k = ::Utils.underscore(spec_k).to_sym

          ruby_k = {
            min: :min_value,
            max: :max_value,
            show_record_id: :show_disk_loc
          }[ruby_k] || ruby_k

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
        op_name = ::Utils.underscore(request['name'])
        args = ::Utils.shallow_snakeize_hash(request['arguments'])
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
        ::Utils.underscore(v).to_sym
      end

      def update
        arguments['update']
      end

      def transform_read_preference(v)
        ::Utils.snakeize_hash(v)
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

      def transformed_options(context)
        opts = options.dup
        if opts[:session]
          opts[:session] = case opts[:session]
          when 'session0'
            unless context.session0
              raise "Trying to use session0 but it is not in context"
            end
            context.session0
          when 'session1'
            unless context.session1
              raise "Trying to use session1 but it is not in context"
            end
            context.session1
          else
            raise "Invalid session name '#{opts[:session]}'"
          end
        end
        opts
      end
    end
  end
end
