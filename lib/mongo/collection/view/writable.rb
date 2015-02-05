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
  class Collection
    class View

      # Defines write related behaviour for collection view.
      #
      # @since 2.0.0
      module Writable

        # Finds a single document in the database via findAndModify and deletes
        # it, returning the original document.
        #
        # @example Find one document and delete it.
        #   view.find_one_and_delete
        #
        # @return [ BSON::Document, nil ] The document, if found.
        #
        # @since 2.0.0
        def find_one_and_delete
          cmd = { :findAndModify => collection.name, :query => selector, :remove => true }
          cmd[:fields] = projection if projection
          cmd[:sort] = sort if sort
          database.command(cmd, options).first['value']
        end

        # Remove documents from the collection.
        #
        # @example Remove multiple documents from the collection.
        #   collection_view.remove_many
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def remove_many
          remove(0)
        end

        # Remove a document from the collection.
        #
        # @example Remove a single document from the collection.
        #   collection_view.remove_one
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def remove_one
          remove(1)
        end

        # Replaces a single document in the database with the new document.
        #
        # @example Replace a single document.
        #   collection_view.replace_one({ name: 'test' })
        #
        # @param [ Hash ] document The document to replace.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def replace_one(document)
          update(document, false)
        end

        # Update documents in the collection.
        #
        # @example Update multiple documents in the collection.
        #   collection_view.update_many('$set' => { name: 'test' })
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_many(spec)
          update(spec, true)
        end

        # Update a single document in the collection.
        #
        # @example Update a single document in the collection.
        #   collection_view.update_one('$set' => { name: 'test' })
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_one(spec)
          update(spec, false)
        end

        private

        def remove(value)
          Operation::Write::Delete.new(
            :delete => { q: selector, limit: value },
            :db_name => collection.database.name,
            :coll_name => collection.name,
            :write_concern => collection.write_concern
          ).execute(next_primary.context)
        end

        def update(spec, multi)
          Operation::Write::Update.new(
            :update => { q: selector, u: spec, multi: multi, upsert: false },
            :db_name => collection.database.name,
            :coll_name => collection.name,
            :write_concern => collection.write_concern
          ).execute(next_primary.context)
        end
      end
    end
  end
end
