# frozen_string_literal: true
# encoding: utf-8

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
        @crypt_handle = Handle.new(kms_providers, kms_tls_options)
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
      # @option options [ Symbol ] query_type Query type to be applied
      # if encryption algorithm is set to "Indexed". Query type should be set
      #   only if encryption algorithm is set to "Indexed". The only allowed
      #   value is :equality.
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
          { 'v': value },
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
        result = Crypt::ExplicitDecryptionContext.new(
          @crypt_handle,
          @encryption_io,
          { 'v': value },
        ).run_state_machine['v']
      end

      def add_key_alt_name(id, key_alt_name)
        @encryption_io.add_key_alt_name(id, key_alt_name)
      end
    end
  end
end
