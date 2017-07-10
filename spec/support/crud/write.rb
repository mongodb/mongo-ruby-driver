# Copyright (C) 2014-2017 MongoDB, Inc.
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
    module Operation

      # Defines common behaviour for running CRUD write operation tests on a
      # collection.
      #
      # @since 2.0.0
      class Write

        # Map of CRUD operation names to method names.
        #
        # @since 2.0.0
        OPERATIONS = { 'deleteMany' => :delete_many,
                       'deleteOne' => :delete_one,
                       'insertMany' => :insert_many,
                       'insertOne' => :insert_one,
                       'replaceOne' => :replace_one,
                       'updateMany' => :update_many,
                       'updateOne' => :update_one,
                       'findOneAndDelete' => :find_one_and_delete,
                       'findOneAndReplace' => :find_one_and_replace,
                       'findOneAndUpdate' => :find_one_and_update,
                       'bulkWrite' => :bulk_write
                     }.freeze

        # Map of operation options to method names.
        #
        # @since 2.0.0
        ARGUMENT_MAP = {
                        :sort => 'sort',
                        :projection => 'projection',
                        :return_document => 'returnDocument',
                        :upsert => 'upsert',
                        :ordered => 'ordered',
                        :write_concern => 'writeConcern',
                        :collation => 'collation'
                       }.freeze

        # The operation name.
        #
        # @return [ String ] name The operation name.
        #
        # @since 2.0.0
        attr_reader :name

        # Instantiate the operation.
        #
        # @return [ Hash ] spec The operation spec.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
          @name = spec['name']
        end

        # Whether the operation is expected to have restuls.
        #
        # @example Whether the operation is expected to have results.
        #   operation.has_results?
        #
        # @return [ true ] If the operation is expected to have results.
        #
        # @since 2.0.0
        def has_results?
          true
        end

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.execute
        #
        # @param [ Collection ] collection The collection to execute
        #   the operation on.
        #
        # @return [ Result, Array<Hash> ] The result of executing the operation.
        #
        # @since 2.0.0
        def execute(collection)
          send(OPERATIONS[name], collection)
        end

        private

        def bulk_write(collection)
          collection.bulk_write(requests, options)
        end

        def delete_many(collection)
          result = collection.delete_many(filter, options)
          { 'deletedCount' => result.deleted_count }
        end

        def delete_one(collection)
          result = collection.delete_one(filter, options)
          { 'deletedCount' => result.deleted_count }
        end

        def insert_many(collection)
          result = collection.insert_many(documents, options)
          { 'insertedIds' => result.inserted_ids }
        end

        def insert_one(collection)
          result = collection.insert_one(document)
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

        def options
          ARGUMENT_MAP.reduce({}) do |opts, (key, value)|
            arguments.key?(value) ? opts.merge!(key => send(key)) : opts
          end
        end

        def collation
          arguments['collation']
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

        def write_concern
          arguments['writeConcern']
        end

        def ordered
          arguments['ordered']
        end

        def filter
          arguments['filter']
        end

        def requests
          arguments['requests'].map do |request|
            case request.keys.first
            when 'insertOne' then
              { insert_one: request['insertOne']['document'] }
            when 'updateOne' then
              update = request['updateOne']
              { update_one: { filter: update['filter'], update: update['update'] }}
            end
          end
        end

        def upsert
          arguments['upsert']
        end

        def return_document
          :after if arguments['returnDocument']
        end

        def update
          arguments['update']
        end

        def arguments
          @spec['arguments']
        end
      end
    end
  end
end
