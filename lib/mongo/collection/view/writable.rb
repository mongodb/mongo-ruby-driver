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
  class Collection
    class View

      # Defines write related behavior for collection view.
      #
      # @since 2.0.0
      module Writable

        # The array filters field constant.
        #
        # @since 2.5.0
        ARRAY_FILTERS = 'array_filters'.freeze

        # Finds a single document in the database via findAndModify and deletes
        # it, returning the original document.
        #
        # @example Find one document and delete it.
        #   view.find_one_and_delete
        #
        # @param [ Hash ] opts The options.
        #
        # @option opts [ Integer ] :max_time_ms The maximum amount of time to allow the command
        #   to run in milliseconds.
        # @option opts [ Hash ] :projection The fields to include or exclude in the returned doc.
        # @option opts [ Hash ] :sort The key and direction pairs by which the result set
        #   will be sorted.
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Defaults to the collection's write concern.
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Session ] :session The session to use.
        #
        # @return [ BSON::Document, nil ] The document, if found.
        #
        # @since 2.0.0
        def find_one_and_delete(opts = {})
          cmd = { :findAndModify => collection.name, :query => filter, :remove => true }
          cmd[:fields] = projection if projection
          cmd[:sort] = sort if sort
          cmd[:maxTimeMS] = max_time_ms if max_time_ms

          with_session(opts) do |session|
            applied_write_concern = applied_write_concern(session)
            cmd[:writeConcern] = applied_write_concern.options if applied_write_concern
            write_with_retry(session, applied_write_concern) do |server, txn_num|
              apply_collation!(cmd, server, opts)
              Operation::Command.new(
                  :selector => cmd,
                  :db_name => database.name,
                  :session => session,
                  :txn_num => txn_num
              ).execute(server, client: client)
            end
          end.first['value']
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
        # @option opts [ Hash ] :write_concern The write concern options.
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
        # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command
        #   to run in milliseconds.
        # @option opts [ Hash ] :projection The fields to include or exclude in the returned doc.
        # @option opts [ Hash ] :sort The key and direction pairs by which the result set
        #   will be sorted.
        # @option opts [ Symbol ] :return_document Either :before or :after.
        # @option opts [ true, false ] :upsert Whether to upsert if the document doesn't exist.
        # @option opts [ true, false ] :bypass_document_validation Whether or
        #   not to skip document level validation.
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Defaults to the collection's write concern.
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Array ] :array_filters A set of filters specifying to which array elements
        # an update should apply.
        # @option opts [ Session ] :session The session to use.
        #
        # @return [ BSON::Document ] The document.
        #
        # @since 2.0.0
        def find_one_and_update(document, opts = {})
          cmd = { :findAndModify => collection.name, :query => filter }
          cmd[:update] = document
          cmd[:fields] = projection if projection
          cmd[:sort] = sort if sort
          cmd[:new] = !!(opts[:return_document] && opts[:return_document] == :after)
          cmd[:upsert] = opts[:upsert] if opts[:upsert]
          cmd[:maxTimeMS] = max_time_ms if max_time_ms
          if opts[:bypass_document_validation]
            cmd[:bypassDocumentValidation] = true
          end

          value = with_session(opts) do |session|
            applied_write_concern = applied_write_concern(opts[:session])
            cmd[:writeConcern] = applied_write_concern.options if applied_write_concern
            write_with_retry(session, applied_write_concern) do |server, txn_num|
              apply_collation!(cmd, server, opts)
              apply_array_filters!(cmd, server, opts)
              Operation::Command.new(
                  :selector => cmd,
                  :db_name => database.name,
                  :session => session,
                  :txn_num => txn_num
              ).execute(server, client: client)
            end
          end.first['value']
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
        # @option opts [ Session ] :session The session to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def delete_many(opts = {})
          delete_doc = { Operation::Q => filter, Operation::LIMIT => 0 }
          with_session(opts) do |session|
            write_concern = write_concern_with_session(session)
            nro_write_with_retry(session, write_concern) do |server|
              apply_collation!(delete_doc, server, opts)
              Operation::Delete.new(
                  :deletes => [ delete_doc ],
                  :db_name => collection.database.name,
                  :coll_name => collection.name,
                  :write_concern => write_concern,
                  :session => session
              ).execute(server, client: client)
            end
          end
        end

        # Remove a document from the collection.
        #
        # @example Remove a single document from the collection.
        #   collection_view.delete_one
        #
        # @param [ Hash ] opts The options.
        #
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Session ] :session The session to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def delete_one(opts = {})
          delete_doc = { Operation::Q => filter, Operation::LIMIT => 1 }
          with_session(opts) do |session|
            write_concern = write_concern_with_session(session)
            write_with_retry(session, write_concern) do |server, txn_num|
              apply_collation!(delete_doc, server, opts)
              Operation::Delete.new(
                  :deletes => [ delete_doc ],
                  :db_name => collection.database.name,
                  :coll_name => collection.name,
                  :write_concern => write_concern,
                  :session => session,
                  :txn_num => txn_num
              ).execute(server, client: client)
            end
          end
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
        # @option opts [ true, false ] :bypass_document_validation Whether or
        #   not to skip document level validation.
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Session ] :session The session to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def replace_one(replacement, opts = {})
          update_doc = { Operation::Q => filter,
                         Operation::U => replacement,
                        }
          if opts[:upsert]
            update_doc['upsert'] = true
          end
          with_session(opts) do |session|
            write_concern = write_concern_with_session(session)
            write_with_retry(session, write_concern) do |server, txn_num|
              apply_collation!(update_doc, server, opts)
              apply_array_filters!(update_doc, server, opts)

              Operation::Update.new(
                  :updates => [ update_doc ],
                  :db_name => collection.database.name,
                  :coll_name => collection.name,
                  :write_concern => write_concern,
                  :bypass_document_validation => !!opts[:bypass_document_validation],
                  :session => session,
                  :txn_num => txn_num
              ).execute(server, client: client)
            end
          end
        end

        # Update documents in the collection.
        #
        # @example Update multiple documents in the collection.
        #   collection_view.update_many('$set' => { name: 'test' })
        #
        # @param [ Hash | Array<Hash> ] spec The update document or pipeline.
        # @param [ Hash ] opts The options.
        #
        # @option opts [ true, false ] :upsert Whether to upsert if the
        #   document doesn't exist.
        # @option opts [ true, false ] :bypass_document_validation Whether or
        #   not to skip document level validation.
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Array ] :array_filters A set of filters specifying to
        #   which array elements an update should apply.
        # @option opts [ Session ] :session The session to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_many(spec, opts = {})
          update_doc = { Operation::Q => filter,
                         Operation::U => spec,
                         Operation::MULTI => true,
                         }
          if opts[:upsert]
            update_doc['upsert'] = true
          end
          with_session(opts) do |session|
            write_concern = write_concern_with_session(session)
            nro_write_with_retry(session, write_concern) do |server|
              apply_collation!(update_doc, server, opts)
              apply_array_filters!(update_doc, server, opts)
              Operation::Update.new(
                  :updates => [ update_doc ],
                  :db_name => collection.database.name,
                  :coll_name => collection.name,
                  :write_concern => write_concern,
                  :bypass_document_validation => !!opts[:bypass_document_validation],
                  :session => session
              ).execute(server, client: client)
            end
          end
        end

        # Update a single document in the collection.
        #
        # @example Update a single document in the collection.
        #   collection_view.update_one('$set' => { name: 'test' })
        #
        # @param [ Hash | Array<Hash> ] spec The update document or pipeline.
        # @param [ Hash ] opts The options.
        #
        # @option opts [ true, false ] :upsert Whether to upsert if the
        #   document doesn't exist.
        # @option opts [ true, false ] :bypass_document_validation Whether or
        #   not to skip document level validation.
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Array ] :array_filters A set of filters specifying to
        #   which array elements an update should apply.
        # @option opts [ Session ] :session The session to use.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_one(spec, opts = {})
          update_doc = { Operation::Q => filter,
                         Operation::U => spec,
                         }
          if opts[:upsert]
            update_doc['upsert'] = true
          end
          with_session(opts) do |session|
            write_concern = write_concern_with_session(session)
            write_with_retry(session, write_concern) do |server, txn_num|
              apply_collation!(update_doc, server, opts)
              apply_array_filters!(update_doc, server, opts)

              Operation::Update.new(
                  :updates => [ update_doc ],
                  :db_name => collection.database.name,
                  :coll_name => collection.name,
                  :write_concern => write_concern,
                  :bypass_document_validation => !!opts[:bypass_document_validation],
                  :session => session,
                  :txn_num => txn_num
              ).execute(server, client: client)
            end
          end
        end

        private

        def apply_array_filters!(doc, server, opts = {})
          if filters = opts[:array_filters] || opts[ARRAY_FILTERS]
            validate_array_filters!(server, filters)
            doc[:arrayFilters] = filters
          end
        end

        def validate_array_filters!(server, filters)
          if filters && !server.with_connection { |connection| connection.features }.array_filters_enabled?
            raise Error::UnsupportedArrayFilters.new
          end
        end

        # Get the write concern for an operation
        #
        # @return [ Mongo::WriteConcern ] The write concern.
        def applied_write_concern(session)
          if wco = options[:write_concern] || options[:write]
            WriteConcern.get(wco)
          else
            write_concern_with_session(session)
          end
        end
      end
    end
  end
end
