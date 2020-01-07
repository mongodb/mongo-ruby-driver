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
    include Crypt::Encrypter

    # Create a new ClientEncryption object with the provided options.
    #
    # @param [ Mongo::Client ] client A Mongo::Client
    #   that is connected to the MongoDB instance where the key vault
    #   collection is stored.
    # @param [ Hash ] options The ClientEncryption options
    #
    # @option options [ String ] :key_vault_namespace The name of the
    #   key vault collection in the format "database.collection"
    # @option options [ Hash ] :kms_providers A hash of key management service
    #   configuration information. Valid hash keys are :local or :aws. There may be
    #   more than one KMS provider specified.
    def initialize(client, options = {})
      setup_encrypter(options.merge(key_vault_client: client))
    end

    # Generates a data key used for encryption/decryption and stores
    # that key in the KMS collection. The generated key is encrypted with
    # the KMS master key.
    #
    # @return [ String ] Base64-encoded UUID string representing the
    #   data key _id
    def create_data_key
      byebug
      result = Crypt::DataKeyContext.new(@crypt_handle).run_state_machine
      byebug

      data_key_document = Hash.from_bson(BSON::ByteBuffer.new(result))
      insert_result = @encryption_io.insert(data_key_document)

      return insert_result.inserted_id.data
    end

    # Encrypts a value using the specified encryption key and algorithm
    #
    # @param [ String|Numeric ] value The value to encrypt
    # @param [ Hash ] opts
    #
    # @option [ String ] :key_id The base64-encoded UUID of the encryption
    #   key as it is stored in the key vault collection
    # @option [ String ] :algorithm The algorithm used to encrypt the value.
    #   Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
    #   or "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
    #
    # @return [ String ] The encrypted value
    #
    # This method is not currently unit tested.
    # Find tests in spec/integration/explicit_encryption_spec.rb
    def encrypt(value, opts={})
      value = { 'v': value }.to_bson.to_s

      Crypt::ExplicitEncryptionContext.new(
        @crypt_handle,
        @encryption_io,
        value,
        opts
      ).run_state_machine
    end

    # Decrypts a value that has already been encrypted
    #
    # @param [ String ] value The value to decrypt
    #
    # @return [ String|Numeric ] The decrypted value
    #
    # This method is not currently unit tested.
    # Find tests in spec/integration/explicit_encryption_spec.rb
    def decrypt(value)
      result = Crypt::ExplicitDecryptionContext.new(
                @crypt_handle,
                @encryption_io,
                value
               ).run_state_machine

      Hash.from_bson(BSON::ByteBuffer.new(result))['v']
    end
  end
end
