# frozen_string_literal: true

# Copyright (C) 2020 MongoDB Inc.
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
  module Crypt
    # An ExplicitEncrypter is an object that performs explicit encryption
    # operations and handles all associated options and instance variables.
    #
    # @api private
    class ExplicitEncrypter
      extend Forwardable

      # Create a new ExplicitEncrypter object.
      #
      # @param [ Mongo::Client ] key_vault_client An instance of Mongo::Client
      #   to connect to the key vault collection.
      # @param [ String ] key_vault_namespace The namespace of the key vault
      #   collection in the format "db_name.collection_name".
      # @param [ Crypt::KMS::Credentials ] kms_providers A hash of key management service
      #   configuration information.
      # @param [ Hash ] kms_tls_options TLS options to connect to KMS
      #   providers. Keys of the hash should be KSM provider names; values
      #   should be hashes of TLS connection options. The options are equivalent
      #   to TLS connection options of Mongo::Client.
      def initialize(key_vault_client, key_vault_namespace, kms_providers, kms_tls_options)
        Crypt.validate_ffi!
        @crypt_handle = Handle.new(
          kms_providers,
          kms_tls_options,
          explicit_encryption_only: true
        )
        @encryption_io = EncryptionIO.new(
          key_vault_client: key_vault_client,
          metadata_client: nil,
          key_vault_namespace: key_vault_namespace
        )
      end

      # Generates a data key used for encryption/decryption and stores
      # that key in the KMS collection. The generated key is encrypted with
      # the KMS master key.
      #
      # @param [ Mongo::Crypt::KMS::MasterKeyDocument ] master_key_document The master
      #   key document that contains master encryption key parameters.
      # @param [ Array<String> | nil ] key_alt_names An optional array of strings specifying
      #   alternate names for the new data key.
      # @param [ String | nil ] key_material Optional 96 bytes to use as
      #   custom key material for the data key being created.
      #   If key_material option is given, the custom key material is used
      #   for encrypting and decrypting data.
      #
      # @return [ BSON::Binary ] The 16-byte UUID of the new data key as a
      #   BSON::Binary object with type :uuid.
      def create_and_insert_data_key(master_key_document, key_alt_names, key_material = nil)
        data_key_document = Crypt::DataKeyContext.new(
          @crypt_handle,
          @encryption_io,
          master_key_document,
          key_alt_names,
          key_material
        ).run_state_machine

        @encryption_io.insert_data_key(data_key_document).inserted_id
      end

      # Encrypts a value using the specified encryption key and algorithm
      #
      # @param [ Object ] value The value to encrypt
      # @param [ Hash ] options
      #
      # @option options [ BSON::Binary ] :key_id A BSON::Binary object of type :uuid
      #   representing the UUID of the encryption key as it is stored in the key
      #   vault collection.
      # @option options [ String ] :key_alt_name The alternate name for the
      #   encryption key.
      # @option options [ String ] :algorithm The algorithm used to encrypt the value.
      #   Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
      #   "AEAD_AES_256_CBC_HMAC_SHA_512-Random", "Indexed", "Unindexed".
      # @option options [ Integer | nil ] :contention_factor Contention factor
      #   to be applied if encryption algorithm is set to "Indexed". If not
      #   provided, it defaults to a value of 0. Contention factor should be set
      #   only if encryption algorithm is set to "Indexed".
      # @option options [ String | nil ] query_type Query type to be applied
      # if encryption algorithm is set to "Indexed". Query type should be set
      #   only if encryption algorithm is set to "Indexed". The only allowed
      #   value is "equality".
      #
      # @note The :key_id and :key_alt_name options are mutually exclusive. Only
      #   one is required to perform explicit encryption.
      #
      # @return [ BSON::Binary ] A BSON Binary object of subtype 6 (ciphertext)
      #   representing the encrypted value
      # @raise [ ArgumentError ] if either contention_factor or query_type
      #   is set, and algorithm is not "Indexed".
      def encrypt(value, options)
        Crypt::ExplicitEncryptionContext.new(
          @crypt_handle,
          @encryption_io,
          { v: value },
          options
        ).run_state_machine['v']
      end

      # Encrypts a Match Expression or Aggregate Expression to query a range index.
      #
      # @example Encrypt Match Expression.
      #   encryption.encrypt_expression(
      #     {'$and' =>  [{'field' => {'$gt' => 10}}, {'field' =>  {'$lt' => 20 }}]}
      #   )
      # @example Encrypt Aggregate Expression.
      #   encryption.encrypt_expression(
      #     {'$and' =>  [{'$gt' => ['$field', 10]}, {'$lt' => ['$field', 20]}}
      #   )
      #   {$and: [{$gt: [<fieldpath>, <value1>]}, {$lt: [<fieldpath>, <value2>]}]
      # Only supported when queryType is "rangePreview" and algorithm is "RangePreview".
      # @note: The Range algorithm is experimental only. It is not intended
      #   for public use. It is subject to breaking changes.
      #
      # @param [ Hash ] expression Expression to encrypt.
      # # @param [ Hash ] options
      # @option options [ BSON::Binary ] :key_id A BSON::Binary object of type :uuid
      #   representing the UUID of the encryption key as it is stored in the key
      #   vault collection.
      # @option options [ String ] :key_alt_name The alternate name for the
      #   encryption key.
      # @option options [ String ] :algorithm The algorithm used to encrypt the
      #   expression. The only allowed value is "RangePreview"
      # @option options [ Integer | nil ] :contention_factor Contention factor
      #   to be applied If not  provided, it defaults to a value of 0.
      # @option options [ String | nil ] query_type Query type to be applied.
      #   The only allowed value is "rangePreview".
      # @option options [ Hash | nil ] :range_opts Specifies index options for
      #   a Queryable Encryption field supporting "rangePreview" queries.
      #   Allowed options are:
      #   - :min
      #   - :max
      #   - :sparsity
      #   - :precision
      #   min, max, sparsity, and range must match the values set in
      #   the encryptedFields of the destination collection.
      #   For double and decimal128, min/max/precision must all be set,
      #   or all be unset.
      #
      # @note The RangePreview algorithm is experimental only. It is not
      # intended for public use.
      #
      # @note The :key_id and :key_alt_name options are mutually exclusive. Only
      #   one is required to perform explicit encryption.
      #
      # @return [ BSON::Binary ] A BSON Binary object of subtype 6 (ciphertext)
      #   representing the encrypted expression.
      #
      # @raise [ ArgumentError ] if disallowed values in options are set.
      def encrypt_expression(expression, options)
        Crypt::ExplicitEncryptionExpressionContext.new(
          @crypt_handle,
          @encryption_io,
          { v: expression },
          options
        ).run_state_machine['v']
      end

      # Decrypts a value that has already been encrypted
      #
      # @param [ BSON::Binary ] value A BSON Binary object of subtype 6 (ciphertext)
      #   that will be decrypted
      #
      # @return [ Object ] The decrypted value
      def decrypt(value)
        Crypt::ExplicitDecryptionContext.new(
          @crypt_handle,
          @encryption_io,
          { v: value }
        ).run_state_machine['v']
      end

      # Adds a key_alt_name for the key in the key vault collection with the given id.
      #
      # @param [ BSON::Binary ] id Id of the key to add new key alt name.
      # @param [ String ] key_alt_name New key alt name to add.
      #
      # @return [ BSON::Document | nil ] Document describing the identified key
      #   before adding the key alt name, or nil if no such key.
      def add_key_alt_name(id, key_alt_name)
        @encryption_io.add_key_alt_name(id, key_alt_name)
      end

      # Removes the key with the given id from the key vault collection.
      #
      # @param [ BSON::Binary ] id Id of the key to delete.
      #
      # @return [ Operation::Result ] The response from the database for the delete_one
      #   operation that deletes the key.
      def_delegators :@encryption_io, :delete_key

      # Finds a single key with the given id.
      #
      # @param [ BSON::Binary ] id Id of the key to get.
      #
      # @return [ BSON::Document | nil ] The found key document or nil
      #   if not found.
      def_delegators :@encryption_io, :get_key

      # Returns a key in the key vault collection with the given key_alt_name.
      #
      # @param [ String ] key_alt_name Key alt name to find a key.
      #
      # @return [ BSON::Document | nil ] The found key document or nil
      #   if not found.
      def_delegators :@encryption_io, :get_key_by_alt_name

      # Returns all keys in the key vault collection.
      #
      # @return [ Collection::View ] Keys in the key vault collection.
      def_delegators :@encryption_io, :get_keys

      # Removes a key_alt_name from a key in the key vault collection with the given id.
      #
      # @param [ BSON::Binary ] id Id of the key to remove key alt name.
      # @param [ String ] key_alt_name Key alt name to remove.
      #
      # @return [ BSON::Document | nil ] Document describing the identified key
      #   before removing the key alt name, or nil if no such key.
      def_delegators :@encryption_io, :remove_key_alt_name

      # Decrypts multiple data keys and (re-)encrypts them with a new master_key,
      #   or with their current master_key if a new one is not given.
      #
      # @param [ Hash ] filter Filter used to find keys to be updated.
      # @param [ Hash ] options
      #
      # @option options [ String ] :provider KMS provider to encrypt keys.
      # @option options [ Hash | nil ] :master_key Document describing master key
      #   to encrypt keys.
      #
      # @return [ Crypt::RewrapManyDataKeyResult ] Result of the operation.
      def rewrap_many_data_key(filter, opts = {})
        validate_rewrap_options!(opts)

        master_key_document = master_key_for_provider(opts)

        rewrap_result = Crypt::RewrapManyDataKeyContext.new(
          @crypt_handle,
          @encryption_io,
          filter,
          master_key_document
        ).run_state_machine

        return RewrapManyDataKeyResult.new(nil) if rewrap_result.nil?

        updates = updates_from_data_key_documents(rewrap_result.fetch('v'))
        RewrapManyDataKeyResult.new(@encryption_io.update_data_keys(updates))
      end

      private

      # Ensures the consistency of the options passed to #rewrap_many_data_keys.
      #
      # @param [ Hash ] opts the options hash to validate
      #
      # @raise [ ArgumentError ] if the options are not consistent or
      #   compatible.
      def validate_rewrap_options!(opts)
        return unless opts.key?(:master_key) && !opts.key?(:provider)

        raise ArgumentError, 'If :master_key is specified, :provider must also be given'
      end

      # If a :provider is given, construct a new master key document
      # with that provider.
      #
      # @param [ Hash ] opts the options hash
      #
      # @option [ String ] :provider KMS provider to encrypt keys.
      #
      # @return [ KMS::MasterKeyDocument | nil ] the new master key document,
      #   or nil if no provider was given.
      def master_key_for_provider(opts)
        return nil unless opts[:provider]

        options = opts.dup
        provider = options.delete(:provider)
        KMS::MasterKeyDocument.new(provider, options)
      end

      # Returns the corresponding update document for each for of the given
      # data key documents.
      #
      # @param [ Array<Hash> ] documents the data key documents
      #
      # @return [ Array<Hash> ] the update documents
      def updates_from_data_key_documents(documents)
        documents.map do |doc|
          {
            update_one: {
              filter: { _id: doc[:_id] },
              update: {
                '$set' => {
                  masterKey: doc[:masterKey],
                  keyMaterial: doc[:keyMaterial]
                },
                '$currentDate' => { updateDate: true },
              },
            }
          }
        end
      end
    end
  end
end
