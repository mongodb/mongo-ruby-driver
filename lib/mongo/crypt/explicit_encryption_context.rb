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
  module Crypt

    # A Context object initialized for explicit encryption
    class ExplicitEncryptionContext < Context

      # Create a new ExplicitEncryptionContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      # @param [ ClientEncryption::IO ] io A instance of the IO class
      #   that implements driver I/O methods required to run the
      #   state machine
      # @param [ String|Integer ] value A value to encrypt
      # @param [ Hash ] options
      #
      # @option [ String ] :key_id The UUID of the data key that
      #   will be used to encrypt the value
      # @option [ String ] :algorithm The algorithm used to encrypt the
      #   value. Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      #   or "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
      #
      # @raises [ ArgumentError|Mongo::Error::CryptError ] If invalid options are provided
      def initialize(mongocrypt, io, value, options={})
        super(mongocrypt, io)

        @value = value
        @options = options

        set_key_id
        set_algorithm
        initialize_ctx
      end

      private

      # Set the key id option on the mongocrypt_ctx_t object and raises
      # an exception if the key_id option is somehow invalid.
      def set_key_id
        unless @options[:key_id]
          raise ArgumentError.new(':key_id option must not be nil')
        end

        binary = Binary.from_data(@options[:key_id])
        success = Binding.mongocrypt_ctx_setopt_key_id(@ctx, binary.ref)

        raise_from_status unless success
      end

      # Set the algorithm option on the mongocrypt_ctx_t object and raises
      # an exception if the algorithm is invalid.
      def set_algorithm
        success = Binding.mongocrypt_ctx_setopt_algorithm(
          @ctx,
          @options[:algorithm],
          -1
        )

        raise_from_status unless success
      end

      # Initializes the mongocrypt_ctx_t object for explicit encryption and
      # passes in the value to be encrypted as a Mongo::Crypt::Binary reference
      def initialize_ctx
        binary = Binary.from_data(@value)
        success = Binding.mongocrypt_ctx_explicit_encrypt_init(@ctx, binary.ref)

        raise_from_status unless success
      end
    end
  end
end
