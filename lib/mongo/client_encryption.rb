# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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
  # ClientEncryption encapsulates explicit operations on a key vault
  # collection that cannot be done directly on a MongoClient. It
  # provides an API for explicitly encrypting and decrypting values,
  # and creating data keys.
  class ClientEncryption
    # Create a new ClientEncryption object with the provided options.
    #
    # @param [ Mongo::Client ] key_vault_client A Mongo::Client
    #   that is connected to the MongoDB instance where the key vault
    #   collection is stored.
    # @param [ Hash ] options The ClientEncryption options.
    #
    # @option options [ String ] :key_vault_namespace The name of the
    #   key vault collection in the format "database.collection".
    # @option options [ Hash ] :kms_providers A hash of key management service
    #   configuration information.
    #   @see Mongo::Crypt::KMS::Credentials for list of options for every
    #   supported provider.
    #   @note There may be more than one KMS provider specified.
    # @option options [ Hash ] :kms_tls_options TLS options to connect to KMS
    #   providers. Keys of the hash should be KSM provider names; values
    #   should be hashes of TLS connection options. The options are equivalent
    #   to TLS connection options of Mongo::Client.
    #   @see Mongo::Client#initialize for list of TLS options.
    #
    # @raise [ ArgumentError ] If required options are missing or incorrectly
    #   formatted.
    def initialize(key_vault_client, options = {})
      @encrypter = Crypt::ExplicitEncrypter.new(
        key_vault_client,
        options[:key_vault_namespace],
        Crypt::KMS::Credentials.new(options[:kms_providers]),
        Crypt::KMS::Validations.validate_tls_options(options[:kms_tls_options])
      )
    end

    # Generates a data key used for encryption/decryption and stores
    # that key in the KMS collection. The generated key is encrypted with
    # the KMS master key.
    #
    # @param [ String ] kms_provider The KMS provider to use. Valid values are
    #   "aws" and "local".
    # @param [ Hash ] options
    #
    # @option options [ Hash ] :master_key Information about the AWS master key.
    #   Required if kms_provider is "aws".
    #   - :region [ String ] The The AWS region of the master key (required).
    #   - :key [ String ] The Amazon Resource Name (ARN) of the master key (required).
    #   - :endpoint [ String ] An alternate host to send KMS requests to (optional).
    #     endpoint should be a host name with an optional port number separated
    #     by a colon (e.g. "kms.us-east-1.amazonaws.com" or
    #     "kms.us-east-1.amazonaws.com:443"). An endpoint in any other format
    #     will not be properly parsed.
    # @option options [ Array<String> ] :key_alt_names An optional array of
    #   strings specifying alternate names for the new data key.
    # @option options [ String | nil ] :key_material Optional
    #   96 bytes to use as custom key material for the data key being created.
    #   If :key_material option is given, the custom key material is used
    #   for encrypting and decrypting data.
    #
    # @return [ BSON::Binary ] The 16-byte UUID of the new data key as a
    #   BSON::Binary object with type :uuid.
    def create_data_key(kms_provider, options={})
      key_document = Crypt::KMS::MasterKeyDocument.new(kms_provider, options)

      key_alt_names = options[:key_alt_names]
      key_material = options[:key_material]
      @encrypter.create_and_insert_data_key(key_document, key_alt_names, key_material)
    end

    # Encrypts a value using the specified encryption key and algorithm.
    #
    # @param [ Object ] value The value to encrypt.
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
    #   representing the encrypted value.
    #
    # @raise [ ArgumentError ] if either contention_factor or query_type
    #   is set, and algorithm is not "Indexed".
    def encrypt(value, options={})
      @encrypter.encrypt(value, options)
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
    #
    # @note The :key_id and :key_alt_name options are mutually exclusive. Only
    #   one is required to perform explicit encryption.
    #
    # @return [ BSON::Binary ] A BSON Binary object of subtype 6 (ciphertext)
    #   representing the encrypted expression.
    #
    # @raise [ ArgumentError ] if disallowed values in options are set.
    def encrypt_expression(expression, options = {})
      @encrypter.encrypt_expression(expression, options)
    end

    # Decrypts a value that has already been encrypted.
    #
    # @param [ BSON::Binary ] value A BSON Binary object of subtype 6 (ciphertext)
    #   that will be decrypted.
    #
    # @return [ Object ] The decrypted value.
    def decrypt(value)
      @encrypter.decrypt(value)
    end

    # Adds a key_alt_name for the key in the key vault collection with the given id.
    #
    # @param [ BSON::Binary ] id Id of the key to add new key alt name.
    # @param [ String ] key_alt_name New key alt name to add.
    #
    # @return [ BSON::Document | nil ] Document describing the identified key
    #   before adding the key alt name, or nil if no such key.
    def add_key_alt_name(id, key_alt_name)
      @encrypter.add_key_alt_name(id, key_alt_name)
    end

    # Removes the key with the given id from the key vault collection.
    #
    # @param [ BSON::Binary ] id Id of the key to delete.
    #
    # @return [ Operation::Result ] The response from the database for the delete_one
    #   operation that deletes the key.
    def delete_key(id)
      @encrypter.delete_key(id)
    end

    # Finds a single key with the given id.
    #
    # @param [ BSON::Binary ] id Id of the key to get.
    #
    # @return [ BSON::Document | nil ] The found key document or nil
    #   if not found.
    def get_key(id)
      @encrypter.get_key(id)
    end

    # Returns a key in the key vault collection with the given key_alt_name.
    #
    # @param [ String ] key_alt_name Key alt name to find a key.
    #
    # @return [ BSON::Document | nil ] The found key document or nil
    #   if not found.
    def get_key_by_alt_name(key_alt_name)
      @encrypter.get_key_by_alt_name(key_alt_name)
    end

    # Returns all keys in the key vault collection.
    #
    # @return [ Collection::View ] Keys in the key vault collection.
    def get_keys
      @encrypter.get_keys
    end
    alias :keys :get_keys

    # Removes a key_alt_name from a key in the key vault collection with the given id.
    #
    # @param [ BSON::Binary ] id Id of the key to remove key alt name.
    # @param [ String ] key_alt_name Key alt name to remove.
    #
    # @return [ BSON::Document | nil ] Document describing the identified key
    #   before removing the key alt name, or nil if no such key.
    def remove_key_alt_name(id, key_alt_name)
      @encrypter.remove_key_alt_name(id, key_alt_name)
    end

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
      @encrypter.rewrap_many_data_key(filter, opts)
    end

    # Create collection with encrypted fields.
    #
    # If :encryption_fields contains a keyId with a null value, a data key
    # will be automatically generated and assigned to keyId value.
    #
    # @note This method does not update the :encrypted_fields_map in the client's
    #   :auto_encryption_options. Therefore, in order to use the collection
    #   created by this method with automatic encryption, the user must create
    #   a new client after calling this function with the :encrypted_fields returned.
    #
    # @param [ Mongo::Database ] database Database to create collection in.
    # @param [ String ] coll_name Name of collection to create.
    # @param [ Hash ] coll_opts Options for collection to create.
    # @param [ String ] kms_provider KMS provider to encrypt fields.
    # @param [ Hash | nil ] master_key Document describing master key to encrypt fields.
    #
    # @return [ Array<Operation::Result, Hash> ] The result of the create
    #   collection operation and the encrypted fields map used to create
    #   the collection.
    def create_encrypted_collection(database, coll_name, coll_opts, kms_provider, master_key)
      raise ArgumentError, 'coll_opts must contain :encrypted_fields' unless coll_opts[:encrypted_fields]

      encrypted_fields = create_data_keys(coll_opts[:encrypted_fields], kms_provider, master_key)
      begin
        new_coll_opts = coll_opts.dup.merge(encrypted_fields: encrypted_fields)
        [database[coll_name].create(new_coll_opts), encrypted_fields]
      rescue Mongo::Error => e
        raise Error::CryptError, "Error creating collection with encrypted fields \
              #{encrypted_fields}: #{e.class}: #{e.message}"
      end
    end

    private

    # Create data keys for fields in encrypted_fields that has :keyId key,
    # but the value is nil.
    #
    # @param [ Hash ] encrypted_fields Encrypted fields map.
    # @param [ String ] kms_provider KMS provider to encrypt fields.
    # @param [ Hash | nil ] master_key Document describing master key to encrypt fields.
    #
    # @return [ Hash ] Encrypted fields map with keyIds for fields
    #   that did not have one.
    def create_data_keys(encrypted_fields, kms_provider, master_key)
      encrypted_fields = encrypted_fields.dup
      # We must return the partially formed encrypted_fields hash if an error
      # occurs - https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/client-side-encryption.rst#create-encrypted-collection-helper
      # Thefore, we do this in a loop instead of using #map.
      encrypted_fields[:fields].size.times do |i|
        field = encrypted_fields[:fields][i]
        next unless field.is_a?(Hash) && field.fetch(:keyId, false).nil?

        begin
          encrypted_fields[:fields][i][:keyId] = create_data_key(kms_provider, master_key: master_key)
        rescue Error::CryptError => e
          raise Error::CryptError, "Error creating data key for field #{field[:path]} \
              with encrypted fields #{encrypted_fields}: #{e.class}: #{e.message}"
        end
      end
      encrypted_fields
    end
  end
end
