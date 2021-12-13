# frozen_string_literal: true
# encoding: utf-8

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
    def initialize(key_vault_client, options={})
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
    #
    # @return [ BSON::Binary ] The 16-byte UUID of the new data key as a
    #   BSON::Binary object with type :uuid.
    def create_data_key(kms_provider, options={})
      key_document = Crypt::KMS::MasterKeyDocument.new(kms_provider, options)
      key_alt_names = options[:key_alt_names]
      @encrypter.create_and_insert_data_key(key_document, key_alt_names)
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
    #   Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
    #   or "AEAD_AES_256_CBC_HMAC_SHA_512-Random".
    #
    # @note The :key_id and :key_alt_name options are mutually exclusive. Only
    #   one is required to perform explicit encryption.
    #
    # @return [ BSON::Binary ] A BSON Binary object of subtype 6 (ciphertext)
    #   representing the encrypted value.
    def encrypt(value, options={})
      @encrypter.encrypt(value, options)
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
  end
end
