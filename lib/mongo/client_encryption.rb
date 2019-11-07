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

    class IO
      def initialize(client, key_vault_db_name, key_vault_coll_name)
        @client = client
        @key_vault_db_name = key_vault_db_name
        @key_vault_coll_name = key_vault_coll_name
      end

      def find_keys(filter)
        # In the Python driver, they yield keys one by one in
        # a cursor block. Doing this for simplicity but should come
        # back to that later.
        filter = Hash.from_bson(BSON::ByteBuffer.new(filter))

        @client
          .use(@key_vault_db_name)[@key_vault_coll_name]
          .find(filter)
          .to_a
      end
    end

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
      validate_key_vault_namespace(options[:key_vault_namespace])

      @client = client
      @key_vault_db_name, @key_vault_coll_name = options[:key_vault_namespace].split('.')
      @io = IO.new(@client, @key_vault_db_name, @key_vault_coll_name)

      @crypt_handle = Crypt::Handle.new(options[:kms_providers])
    end

    # Closes the underlying crypt_handle object and cleans
    # up resources
    #
    # @return [ true ] Always true
    def close
      # Will eventually revisit the experience of using a
      # Mongo::Crypt::Handle object -- having to close it manually
      # is not the best.
      @crypt_handle.close if @crypt_handle

      @crypt_handle = nil
      @client = nil
      @key_vault_namespace = nil
      @io = nil

      true
    end

    # Generates a data key used for encryption/decryption and stores
    # that key in the KMS collection. The generated key is encrypted with
    # the KMS master key.
    #
    # @return [ BSON::Binary ] UUID representing the data key _id
    def create_data_key
      result = nil

      Crypt::DataKeyContext.with_context(@crypt_handle.ref) do |context|
        result = context.run_state_machine
      end

      data_key_document = Hash.from_bson(BSON::ByteBuffer.new(result))
      insert_result = @client.use(@key_vault_db_name)[@key_vault_coll_name].insert_one(data_key_document)

      return insert_result.inserted_id
    end

    # Encrypts a value using the specified encryption key and algorithm
    #
    # @param [ String|Numeric ] value The value to encrypt
    # @param [ Hash ] opts
    #
    # @option [ BSON::Binary ] :key_id The UUID of the encryption key as
    #   it is stored in the key vault collection
    # @option [ String ] :algorithm The algorithm used to encrypt the value.
    #   Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
    #   or "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
    #
    # @return [ BSON::Binary ] The encrypted value
    def encrypt(value, opts={})
      result = nil
      value = BSON::Binary.new({ 'v': value }.to_bson.to_s)

      Crypt::ExplicitEncryptionContext.with_context(@crypt_handle.ref, @io, value, opts) do |context|
        result = context.run_state_machine
      end

      return BSON::Binary.new(result)
    end

    # Decrypts a value that has already been encrypted
    #
    # @param [ BSON::Binary ] value The value to decrypt
    #
    # @return [ String|Numeric ] The decrypted value
    def decrypt(value)
      result = nil

      Crypt::ExplicitDecryptionContext.with_context(@crypt_handle.ref, @io, value) do |context|
        result = context.run_state_machine
      end

      Hash.from_bson(BSON::ByteBuffer.new(result))['v']
    end

    private

    # Validates that the key_vault_namespace exists and is in the format database.collection
    def validate_key_vault_namespace(key_vault_namespace)
      unless key_vault_namespace
        raise ArgumentError.new('The :key_vault_namespace option cannot be nil.')
      end

      unless key_vault_namespace.split('.').length == 2
        raise ArgumentError.new(
          "#{key_vault_namespace} is an invalid key vault namespace." +
          "The :key_vault_namespace option must be in the format database.collection"
        )
      end
    end
  end
end
