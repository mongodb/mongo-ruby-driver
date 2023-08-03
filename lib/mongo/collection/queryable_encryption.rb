# frozen_string_literal: true

# Copyright (C) 2014-2022 MongoDB Inc.
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
    # This module contains methods for creating and dropping auxiliary collections
    # for queryable encryption.
    #
    # @api private
    module QueryableEncryption
      # The minimum wire version for QE2 support
      QE2_MIN_WIRE_VERSION = 21

      # Creates auxiliary collections and indices for queryable encryption if necessary.
      #
      # @param [ Hash | nil ] encrypted_fields Encrypted fields hash that was
      #   provided to `create` collection helper.
      # @param [ Client ] client Mongo client to be used to create auxiliary collections.
      # @param [ Session ] session Session to be used to create auxiliary collections.
      #
      # @return [ Result ] The result of provided block.
      def maybe_create_qe_collections(encrypted_fields, client, session)
        encrypted_fields = encrypted_fields_from(encrypted_fields)
        return yield if encrypted_fields.empty?

        server = next_primary(nil, session)
        context = Operation::Context.new(client: client, session: session)
        server.with_connection do |connection|
          check_wire_version!(connection)
          emm_collections(encrypted_fields).each do |coll|
            create_operation_for(coll)
              .execute_with_connection(connection, context: context)
          end
        end

        yield(encrypted_fields).tap do |result|
          indexes.create_one(__safeContent__: 1) if result
        end
      end

      # Drops auxiliary collections and indices for queryable encryption if necessary.
      #
      # @param [ Hash | nil ] encrypted_fields Encrypted fields hash that was
      #   provided to `create` collection helper.
      # @param [ Client ] client Mongo client to be used to drop auxiliary collections.
      # @param [ Session ] session Session to be used to drop auxiliary collections.
      #
      # @return [ Result ] The result of provided block.
      def maybe_drop_emm_collections(encrypted_fields, client, session)
        encrypted_fields = if encrypted_fields
                             encrypted_fields
                           elsif encrypted_fields_map
                             encrypted_fields_for_drop_from_map
                           else
                             {}
                           end

        return yield if encrypted_fields.empty?

        emm_collections(encrypted_fields).each do |coll|
          context = Operation::Context.new(client: client, session: session)
          operation = Operation::Drop.new(
            selector: { drop: coll },
            db_name: database.name,
            session: session
          )
          do_drop(operation, session, context)
        end

        yield
      end

      private

      # Checks if names for auxiliary collections are set and returns them,
      # otherwise returns default names.
      #
      # @param [ Hash ] encrypted_fields Encrypted fields hash.
      #
      # @return [ Array <String> ] Array of auxiliary collections names.
      def emm_collections(encrypted_fields)
        [
          encrypted_fields['escCollection'] || "enxcol_.#{name}.esc",
          encrypted_fields['ecocCollection'] || "enxcol_.#{name}.ecoc",
        ]
      end

      # Creating encrypted collections is only supported on 7.0.0 and later
      # (wire version 21+).
      #
      # @param [ Mongo::Connection ] connection The connection to check
      #   the wire version of.
      #
      # @raise [ Mongo::Error ] if the wire version is not
      #   recent enough
      def check_wire_version!(connection)
        return unless connection.description.max_wire_version < QE2_MIN_WIRE_VERSION

        raise Mongo::Error,
              'Driver support of Queryable Encryption is incompatible with server. ' \
              'Upgrade server to use Queryable Encryption.'
      end

      # Tries to return the encrypted fields from the argument. If the argument
      # is nil, tries to find the encrypted fields from the
      # encrypted_fields_map.
      #
      # @param [ Hash | nil ] fields the encrypted fields
      #
      # @return [ Hash ] the encrypted fields
      def encrypted_fields_from(fields)
        fields ||
          (encrypted_fields_map && encrypted_fields_map[namespace]) ||
          {}
      end

      # Tries to return the encrypted fields from the {{encrypted_fields_map}}
      # value, for the current namespace.
      #
      # @return [ Hash | nil ] the encrypted fields, if found
      def encrypted_fields_for_drop_from_map
        encrypted_fields_map[namespace] ||
          database.list_collections(filter: { name: name })
                  .first
                  &.fetch(:options, {})
                  &.fetch(:encryptedFields, {}) ||
          {}
      end

      # Returns a new create operation for the given collection.
      #
      # @param [ String ] coll the name of the collection to create.
      #
      # @return [ Operation::Create ] the new create operation.
      def create_operation_for(coll)
        Operation::Create.new(
          selector: {
            create: coll,
            clusteredIndex: {
              key: { _id: 1 },
              unique: true
            }
          },
          db_name: database.name
        )
      end
    end
  end
end
