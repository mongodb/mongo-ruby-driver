# Copyright (C) 2019 MongoDB, Inc.
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
    include Crypt::ExplicitEncrypter

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
    #   configuration information. Valid hash keys are :local or :aws. There
    #   may be more than one KMS provider specified.
    def initialize(key_vault_client, options = {})
      setup_encrypter(options.merge(key_vault_client: key_vault_client))
    end

    # Generates a data key used for encryption/decryption and stores
    # that key in the KMS collection. The generated key is encrypted with
    # the KMS master key.
    #
    # @param [ String ] kms_provider The KMS provider to use. Valid values are
    #   "aws" and "local".
    # @params [ Hash ] options
    #
    # @option options [ Hash ] :master_key Information about the AWS master key. Required
    #   if kms_provider is "aws".
    #   - :region [ String ] The The AWS region of the master key (required).
    #   - :key [ String ] The Amazon Resource Name (ARN) of the master key (required).
    #   - :endpoint [ String ] An alternate host to send KMS requests to (optional).
    #     endpoint should be a host name with an optional port number separated
    #     by a colon (e.g. "kms.us-east-1.amazonaws.com" or
    #     "kms.us-east-1.amazonaws.com:443"). An endpoint in any other format
    #     will not be properly parsed.
    # @option options [ Array<String> ] :key_alt_names An optional array of strings specifying
    #   alternate names for the new data key.
    #
    # @return [ String ] Base64-encoded UUID string representing the
    #   data key _id
    def create_data_key(kms_provider, options={})
      data_key_document = Crypt::DataKeyContext.new(
        @crypt_handle,
        @encryption_io,
        kms_provider,
        options
      ).run_state_machine

      insert_result = @encryption_io.insert(data_key_document)

      return insert_result.inserted_id.data
    end

    # Encrypts a value using the specified encryption key and algorithm
    #
    # @param [ Object ] value The value to encrypt
    # @param [ Hash ] options
    #
    # @option options [ String ] :key_id The base64-encoded UUID of the encryption
    #   key as it is stored in the key vault collection
    # @option options [ String ] :key_alt_name The alternate name for the
    #   encryption key.
    # @option options [ String ] :algorithm The algorithm used to encrypt the value.
    #   Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
    #   or "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
    #
    # @note The :key_id and :key_alt_name options are mutually exclusive. Only
    #   one is required to perform explicit encryption.
    #
    # @return [ BSON::Binary ] A BSON Binary object of subtype 6 (ciphertext)
    #   representing the encrypted value
    def encrypt(value, options={})
      doc = { 'v': value }

      Crypt::ExplicitEncryptionContext.new(
        @crypt_handle,
        @encryption_io,
        doc,
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
      doc = { 'v': value }

      result = Crypt::ExplicitDecryptionContext.new(
        @crypt_handle,
        @encryption_io,
        doc,
      ).run_state_machine['v']
    end
  end
end
