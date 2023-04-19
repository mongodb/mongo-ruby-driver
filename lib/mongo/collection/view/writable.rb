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
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Session ] :session The session to use.
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ BSON::Document, nil ] The document, if found.
        #
        # @since 2.0.0
        def find_one_and_delete(opts = {})
          with_session(opts) do |session|
            write_concern = if opts[:write_concern]
              WriteConcern.get(opts[:write_concern])
            else
              write_concern_with_session(session)
            end

            QueryCache.clear_namespace(collection.namespace)

            cmd = {
              findAndModify: collection.name,
              query: filter,
              remove: true,
              fields: projection,
              sort: sort,
              maxTimeMS: max_time_ms,
              bypassDocumentValidation: opts[:bypass_document_validation],
              hint: opts[:hint],
              collation: opts[:collation] || opts['collation'] || collation,
              let: opts[:let],
              comment: opts[:comment],
            }.compact

            context = Operation::Context.new(client: client, session: session)
            write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              gte_4_4 = connection.server.description.server_version_gte?('4.4')
              if !gte_4_4 && opts[:hint] && write_concern && !write_concern.acknowledged?
                raise Error::UnsupportedOption.hint_error(unacknowledged_write: true)
              end

              Operation::WriteCommand.new(
                selector: cmd,
                db_name: database.name,
                write_concern: write_concern,
                session: session,
                txn_num: txn_num,
              ).execute_with_connection(connection, context: context)
            end
          end.first&.fetch('value', nil)
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
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
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
        # @option opts [ Hash ] :collation The collation to use.
        # @option opts [ Array ] :array_filters A set of filters specifying to which array elements
        # an update should apply.
        # @option opts [ Session ] :session The session to use.
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ BSON::Document | nil ] The document or nil if none is found.
        #
        # @since 2.0.0
        def find_one_and_update(document, opts = {})
          value = with_session(opts) do |session|
            write_concern = if opts[:write_concern]
              WriteConcern.get(opts[:write_concern])
            else
              write_concern_with_session(session)
            end

            QueryCache.clear_namespace(collection.namespace)

            cmd = {
              findAndModify: collection.name,
              query: filter,
              arrayFilters: opts[:array_filters] || opts['array_filters'],
              update: document,
              fields: projection,
              sort: sort,
              new: !!(opts[:return_document] && opts[:return_document] == :after),
              upsert: opts[:upsert],
              maxTimeMS: max_time_ms,
              bypassDocumentValidation: opts[:bypass_document_validation],
              hint: opts[:hint],
              collation: opts[:collation] || opts['collation'] || collation,
              let: opts[:let],
              comment: opts[:comment]
            }.compact

            context = Operation::Context.new(client: client, session: session)
            write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              gte_4_4 = connection.server.description.server_version_gte?('4.4')
              if !gte_4_4 && opts[:hint] && write_concern && !write_concern.acknowledged?
                raise Error::UnsupportedOption.hint_error(unacknowledged_write: true)
              end

              Operation::WriteCommand.new(
                selector: cmd,
                db_name: database.name,
                write_concern: write_concern,
                session: session,
                txn_num: txn_num,
              ).execute_with_connection(connection, context: context)
            end
          end.first&.fetch('value', nil)
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
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def delete_many(opts = {})
          with_session(opts) do |session|
            write_concern = if opts[:write_concern]
              WriteConcern.get(opts[:write_concern])
            else
              write_concern_with_session(session)
            end

            QueryCache.clear_namespace(collection.namespace)

            delete_doc = {
              Operation::Q => filter,
              Operation::LIMIT => 0,
              hint: opts[:hint],
              collation: opts[:collation] || opts['collation'] || collation,
            }.compact

            context = Operation::Context.new(client: client, session: session)
            nro_write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              gte_4_4 = connection.server.description.server_version_gte?('4.4')
              if !gte_4_4 && opts[:hint] && write_concern && !write_concern.acknowledged?
                raise Error::UnsupportedOption.hint_error(unacknowledged_write: true)
              end

              Operation::Delete.new(
                deletes: [ delete_doc ],
                db_name: collection.database.name,
                coll_name: collection.name,
                write_concern: write_concern,
                bypass_document_validation: !!opts[:bypass_document_validation],
                session: session,
                let: opts[:let],
                comment: opts[:comment],
              ).execute_with_connection(connection, context: context)
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
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def delete_one(opts = {})
          with_session(opts) do |session|
            write_concern = if opts[:write_concern]
              WriteConcern.get(opts[:write_concern])
            else
              write_concern_with_session(session)
            end

            QueryCache.clear_namespace(collection.namespace)

            delete_doc = {
              Operation::Q => filter,
              Operation::LIMIT => 1,
              hint: opts[:hint],
              collation: opts[:collation] || opts['collation'] || collation,
            }.compact

            context = Operation::Context.new(client: client, session: session)
            write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              gte_4_4 = connection.server.description.server_version_gte?('4.4')
              if !gte_4_4 && opts[:hint] && write_concern && !write_concern.acknowledged?
                raise Error::UnsupportedOption.hint_error(unacknowledged_write: true)
              end

              Operation::Delete.new(
                deletes: [ delete_doc ],
                db_name: collection.database.name,
                coll_name: collection.name,
                write_concern: write_concern,
                bypass_document_validation: !!opts[:bypass_document_validation],
                session: session,
                txn_num: txn_num,
                let: opts[:let],
                comment: opts[:comment],
              ).execute_with_connection(connection, context: context)
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
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def replace_one(replacement, opts = {})
          with_session(opts) do |session|
            write_concern = if opts[:write_concern]
              WriteConcern.get(opts[:write_concern])
            else
              write_concern_with_session(session)
            end
            validate_replacement_documents!(replacement)

            QueryCache.clear_namespace(collection.namespace)

            update_doc = {
              Operation::Q => filter,
              arrayFilters: opts[:array_filters] || opts['array_filters'],
              Operation::U => replacement,
              hint: opts[:hint],
              collation: opts[:collation] || opts['collation'] || collation,
            }.compact
            if opts[:upsert]
              update_doc['upsert'] = true
            end

            context = Operation::Context.new(client: client, session: session)
            write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              gte_4_2 = connection.server.description.server_version_gte?('4.2')
              if !gte_4_2 && opts[:hint] && write_concern && !write_concern.acknowledged?
                raise Error::UnsupportedOption.hint_error(unacknowledged_write: true)
              end

              Operation::Update.new(
                updates: [ update_doc ],
                db_name: collection.database.name,
                coll_name: collection.name,
                write_concern: write_concern,
                bypass_document_validation: !!opts[:bypass_document_validation],
                session: session,
                txn_num: txn_num,
                let: opts[:let],
                comment: opts[:comment],
              ).execute_with_connection(connection, context: context)
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
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_many(spec, opts = {})
          with_session(opts) do |session|
            write_concern = if opts[:write_concern]
              WriteConcern.get(opts[:write_concern])
            else
              write_concern_with_session(session)
            end
            validate_update_documents!(spec)

            QueryCache.clear_namespace(collection.namespace)

            update_doc = {
              Operation::Q => filter,
              arrayFilters: opts[:array_filters] || opts['array_filters'],
              Operation::U => spec,
              Operation::MULTI => true,
              hint: opts[:hint],
              collation: opts[:collation] || opts['collation'] || collation,
            }.compact
            if opts[:upsert]
              update_doc['upsert'] = true
            end

            context = Operation::Context.new(client: client, session: session)
            nro_write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              gte_4_2 = connection.server.description.server_version_gte?('4.2')
              if !gte_4_2 && opts[:hint] && write_concern && !write_concern.acknowledged?
                raise Error::UnsupportedOption.hint_error(unacknowledged_write: true)
              end

              Operation::Update.new(
                updates: [ update_doc ],
                db_name: collection.database.name,
                coll_name: collection.name,
                write_concern: write_concern,
                bypass_document_validation: !!opts[:bypass_document_validation],
                session: session,
                let: opts[:let],
                comment: opts[:comment],
              ).execute_with_connection(connection, context: context)
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
        # @option opts [ Hash | String ] :hint The index to use for this operation.
        #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
        # @option opts [ Hash ] :write_concern The write concern options.
        #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
        # @option options [ Hash ] :let Mapping of variables to use in the command.
        #   See the server documentation for details.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        #
        # @return [ Result ] The response from the database.
        #
        # @since 2.0.0
        def update_one(spec, opts = {})
          with_session(opts) do |session|
            write_concern = if opts[:write_concern]
              WriteConcern.get(opts[:write_concern])
            else
              write_concern_with_session(session)
            end
            validate_update_documents!(spec)

            QueryCache.clear_namespace(collection.namespace)

            update_doc = {
              Operation::Q => filter,
              arrayFilters: opts[:array_filters] || opts['array_filters'],
              Operation::U => spec,
              hint: opts[:hint],
              collation: opts[:collation] || opts['collation'] || collation,
            }.compact
            if opts[:upsert]
              update_doc['upsert'] = true
            end

            context = Operation::Context.new(client: client, session: session)
            write_with_retry(write_concern, context: context) do |connection, txn_num, context|
              gte_4_2 = connection.server.description.server_version_gte?('4.2')
              if !gte_4_2 && opts[:hint] && write_concern && !write_concern.acknowledged?
                raise Error::UnsupportedOption.hint_error(unacknowledged_write: true)
              end

              Operation::Update.new(
                updates: [ update_doc ],
                db_name: collection.database.name,
                coll_name: collection.name,
                write_concern: write_concern,
                bypass_document_validation: !!opts[:bypass_document_validation],
                session: session,
                txn_num: txn_num,
                let: opts[:let],
                comment: opts[:comment],
              ).execute_with_connection(connection, context: context)
            end
          end
        end

        private

        # Checks the update documents to make sure they only have atomic modifiers.
        # Note that as per the spec, we only have to examine the first element
        # in the update document.
        #
        # @param [ Hash | Array<Hash> ] spec The update document or pipeline.
        #
        # @raise [ Error::InvalidUpdateDocument ] if the first key in the
        #   document does not start with a $.
        def validate_update_documents!(spec)
          if update = spec.is_a?(Array) ? spec&.first : spec
            if key = update.keys&.first
              unless key.to_s.start_with?("$")
                if Mongo.validate_update_replace
                  raise Error::InvalidUpdateDocument.new(key: key)
                else
                  Error::InvalidUpdateDocument.warn(Logger.logger, key)
                end
              end
            end
          end
        end

        # Check the replacement documents to make sure they don't have atomic
        # modifiers. Note that as per the spec, we only have to examine the
        # first element in the replacement document.
        #
        # @param [ Hash | Array<Hash> ] spec The replacement document or pipeline.
        #
        # @raise [ Error::InvalidUpdateDocument ] if the first key in the
        #   document does not start with a $.
        def validate_replacement_documents!(spec)
          if replace = spec.is_a?(Array) ? spec&.first : spec
            if key = replace.keys&.first
              if key.to_s.start_with?("$")
                if Mongo.validate_update_replace
                  raise Error::InvalidReplacementDocument.new(key: key)
                else
                  Error::InvalidReplacementDocument.warn(Logger.logger, key)
                end
              end
            end
          end
        end
      end
    end
  end
end
