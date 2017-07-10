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
        # @param [ Hash ] opts The options.
        #
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ BSON::Document, nil ] The document, if found.
        #
        # @since 2.0.0
        def find_one_and_delete(opts = {})
          cmd = { :findandmodify => collection.name, :query => filter, :remove => true }
          cmd[:fields] = projection if projection
          cmd[:sort] = sort if sort
          cmd[:maxTimeMS] = max_time_ms if max_time_ms
          cmd[:writeConcern] = write_concern.options if write_concern

          write_with_retry do
            server = next_primary
            apply_collation!(cmd, server, opts)

            Operation::Commands::Command.new({
                                              :selector => cmd,
                                              :db_name => database.name
                                             }).execute(server).first['value']
          end
        end

        # Finds a single document and replaces it.
        #
        # @example Find a document and replace it, returning the original.
        #   view.find_one_and_replace({ name: 'test' }, :return_document => :before)
        #
        # @example Find a document and replace it, returning the new document.
        #   view.find_one_and_replace({ name: 'test' }, :return_document => :after)
        #
        # @param [ BSON::Document ] replacement The replacement.
        # @param [ Hash ] opts The options.
        #
        # @option opts [ Symbol ] :return_document Either :before or :after.
        # @option opts [ true, false ] :upsert Whether to upsert if the document doesn't exist.
        # @option opts [ true, false ] :bypass_document_validation Whether or
        #   not to skip document level validation.
        # @option options [ Hash ] :write_concern The write concern options.
        #   Defaults to the collection's write concern.
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ BSON::Document ] The document.
        #
        # @since 2.0.0
        def find_one_and_replace(replacement, opts = {})
          find_one_and_update(replacement, opts)
        end

        # Finds a single document and updates it.
        #
        # @example Find a document and update it, returning the original.
        #   view.find_one_and_update({ "$set" => { name: 'test' }}, :return_document => :before)
        #
        # @param [ BSON::Document ] document The updates.
        # @param [ Hash ] opts The options.
        #
        # @option opts [ Symbol ] :return_document Either :before or :after.
        # @option opts [ true, false ] :upsert Whether to upsert if the document doesn't exist.
        # @option opts [ true, false ] :bypass_document_validation Whether or
        #   not to skip document level validation.
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Defaults to the collection's write concern.
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ BSON::Document ] The document.
        #
        # @since 2.0.0
        def find_one_and_update(document, opts = {})
          cmd = { :findandmodify => collection.name, :query => filter }
          cmd[:update] = document
          cmd[:fields] = projection if projection
          cmd[:sort] = sort if sort
          cmd[:new] = !!(opts[:return_document] && opts[:return_document] == :after)
          cmd[:upsert] = opts[:upsert] if opts[:upsert]
          cmd[:maxTimeMS] = max_time_ms if max_time_ms
          cmd[:bypassDocumentValidation] = !!opts[:bypass_document_validation]
          cmd[:writeConcern] = write_concern.options if write_concern

          value = write_with_retry do
            server = next_primary
            apply_collation!(cmd, server, opts)

            Operation::Commands::Command.new({
                                              :selector => cmd,
                                              :db_name => database.name
                                             }).execute(server).first['value']
          end
          value unless value.nil? || value.empty?
        end

        # Remove documents from the collection.
        #
        # @example Remove multiple documents from the collection.
        #   collection_view.delete_many
        #
        # @param [ Hash ] opts The options.
        #
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def delete_many(opts = {})
          remove(0, opts)
        end

        # Remove a document from the collection.
        #
        # @example Remove a single document from the collection.
        #   collection_view.delete_one
        #
        # @param [ Hash ] opts The options.
        #
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def delete_one(opts = {})
          remove(1, opts)
        end

        # Replaces a single document in the database with the new document.
        #
        # @example Replace a single document.
        #   collection_view.replace_one({ name: 'test' })
        #
        # @param [ Hash ] replacement The replacement document.
        # @param [ Hash ] opts The options.
        #
        # @option opts [ true, false ] :upsert Whether to upsert if the
        #   document doesn't exist.
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def replace_one(replacement, opts = {})
          update(replacement, false, opts)
        end

        # Update documents in the collection.
        #
        # @example Update multiple documents in the collection.
        #   collection_view.update_many('$set' => { name: 'test' })
        #
        # @param [ Hash ] spec The update statement.
        # @param [ Hash ] opts The options.
        #
        # @option opts [ true, false ] :upsert Whether to upsert if the
        #   document doesn't exist.
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_many(spec, opts = {})
          update(spec, true, opts)
        end

        # Update a single document in the collection.
        #
        # @example Update a single document in the collection.
        #   collection_view.update_one('$set' => { name: 'test' })
        #
        # @param [ Hash ] spec The update statement.
        # @param [ Hash ] opts The options.
        #
        # @option opts [ true, false ] :upsert Whether to upsert if the
        #   document doesn't exist.
        # @option opts [ Hash ] :collation The collation to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_one(spec, opts = {})
          update(spec, false, opts)
        end

        private

        def remove(value, opts = {})
          delete_doc = { Operation::Q => filter, Operation::LIMIT => value }
          write_with_retry do
            server = next_primary
            apply_collation!(delete_doc, server, opts)

            Operation::Write::Delete.new(
              :delete => delete_doc,
              :db_name => collection.database.name,
              :coll_name => collection.name,
              :write_concern => collection.write_concern
            ).execute(server)
          end
        end

        def update(spec, multi, opts)
          update_doc = { Operation::Q => filter,
                         Operation::U => spec,
                         Operation::MULTI => multi,
                         Operation::UPSERT => !!opts[:upsert] }
          write_with_retry do
            server = next_primary
            apply_collation!(update_doc, server, opts)

            Operation::Write::Update.new(
              :update => update_doc,
              :db_name => collection.database.name,
              :coll_name => collection.name,
              :write_concern => collection.write_concern,
              :bypass_document_validation => !!opts[:bypass_document_validation]
            ).execute(server)
          end
        end
      end
    end
  end
end
