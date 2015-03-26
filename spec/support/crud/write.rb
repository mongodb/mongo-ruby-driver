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
                       'findOneAndUpdate' => :find_one_and_update
                     }

        # Map of operation options to method names.
        #
        # @since 2.0.0
        ARGUMENT_MAP = {
                        :sort => 'sort',
                        :projection => 'projection'
                       }

        # Operations that need a check if results on < 2.6 will match.
        #
        # @since 2.0.0
        REQUIRES_2_6 = ['findOneAndReplace',
                        'updateMany',
                        'updateOne',
                        'replaceOne']
  
        # The operation name.
        #
        # @return [ String ] name The operation name.
        #
        # @since 2.0.0
        attr_reader :name

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

        # Whether this operation requires >= 2.6 to be tested.
        #
        # @example Determine whether this operation requires >= 2.6.
        #   operation.requires_2_6?(collection)
        #
        # @param [ Collection ] collection The collection the operation
        #   should be executed on.
        #
        # @return [ true, false ] Whether this operation requires 2.6
        #   to be tested.
        #
        # @since 2.0.0
        def requires_2_6?(collection)
          REQUIRES_2_6.include?(name) && upsert
        end
  
        private
  
        def delete_many(collection)
          result = collection.find(filter).delete_many
          { 'deletedCount' => result.deleted_count }
        end

        def delete_one(collection)
          result = collection.find(filter).delete_one
          { 'deletedCount' => result.deleted_count }
        end

        def insert_many(collection)
          result = collection.insert_many(documents)
          { 'insertedIds' => result.inserted_ids }
        end

        def insert_one(collection)
          result = collection.insert_one(document)
          { 'insertedId' => result.inserted_id }
        end

        def update_return_doc(result)
          return_doc = { 'upsertedId' => result.upserted_id } if upsert
          (return_doc || {}).merge!({ 'matchedCount' => result.matched_count,
                                      'modifiedCount' => result.modified_count })
        end

        def replace_one(collection)
          result = collection.find(filter).replace_one(replacement, upsert: upsert)
          update_return_doc(result)
        end

        def update_many(collection)
          result = collection.find(filter).update_many(update, upsert: upsert)
          update_return_doc(result)
        end

        def update_one(collection)
          result = collection.find(filter).update_one(update, upsert: upsert)
          update_return_doc(result)
        end

        def find_one_and_delete(collection)
          view = collection.find(filter)
          ARGUMENT_MAP.each do |key, value|
            view = view.send(key, arguments[value]) if arguments[value]
          end
          view.find_one_and_delete
        end

        def find_one_and_replace(collection)
          view = collection.find(filter)
          ARGUMENT_MAP.each do |key, value|
            view = view.send(key, arguments[value]) if arguments[value]
          end
          view.find_one_and_replace(replacement, upsert: upsert, return_document: return_document)
        end

        def find_one_and_update(collection)
          view = collection.find(filter)
          ARGUMENT_MAP.each do |key, value|
            view = view.send(key, arguments[value]) if arguments[value]
          end
          view.find_one_and_update(update, upsert: upsert, return_document: return_document)
        end

        def replacement
          arguments['replacement']
        end

        def documents
          arguments['documents']
        end

        def document
          arguments['document']
        end

        def filter
          arguments['filter']
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