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
  module Crypt

    # A Context object initialized for explicit encryption
    #
    # @api private
    class ExplicitEncryptionContext < Context

      # Create a new ExplicitEncryptionContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      # @param [ ClientEncryption::IO ] io A instance of the IO class
      #   that implements driver I/O methods required to run the
      #   state machine
      # @param [ BSON::Document ] doc A document to encrypt
      # @param [ Hash ] options
      #
      # @option options [ BSON::Binary ] :key_id A BSON::Binary object of type
      #   :uuid representing the UUID of the data key to use for encryption.
      # @option options [ String ] :key_alt_name The alternate name of the data key
      #   that will be used to encrypt the value.
      # @option options [ String ] :algorithm The algorithm used to encrypt the
      #   value. Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      #   or "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
      #
      # @raise [ ArgumentError|Mongo::Error::CryptError ] If invalid options are provided
      def initialize(mongocrypt, io, doc, options={})
        super(mongocrypt, io)

        if options[:key_id].nil? && options[:key_alt_name].nil?
          raise ArgumentError.new(
            'The :key_id and :key_alt_name options cannot both be nil. ' +
            'Specify a :key_id option or :key_alt_name option (but not both)'
          )
        end

        if options[:key_id] && options[:key_alt_name]
          raise ArgumentError.new(
            'The :key_id and :key_alt_name options cannot both be present. ' +
            'Identify the data key by specifying its id with the :key_id ' +
            'option or specifying its alternate name with the :key_alt_name option'
          )
        end

        # Set the key id or key alt name option on the mongocrypt_ctx_t object
        # and raise an exception if the key_id or key_alt_name is invalid.
        if options[:key_id]
          unless options[:key_id].is_a?(BSON::Binary) &&
            options[:key_id].type == :uuid
              raise ArgumentError.new(
                "Expected the :key_id option to be a BSON::Binary object with " +
                "type :uuid. #{options[:key_id]} is an invalid :key_id option"
              )
          end

          Binding.ctx_setopt_key_id(self, options[:key_id].data)
        elsif options[:key_alt_name]
          unless options[:key_alt_name].is_a?(String)
            raise ArgumentError.new(':key_alt_name option must be a String')
          end
          Binding.ctx_setopt_key_alt_names(self, [options[:key_alt_name]])
        end

        # Set the algorithm option on the mongocrypt_ctx_t object and raises
        # an exception if the algorithm is invalid.
        Binding.ctx_setopt_algorithm(self, options[:algorithm])

        # Initializes the mongocrypt_ctx_t object for explicit encryption and
        # passes in the value to be encrypted.
        Binding.ctx_explicit_encrypt_init(self, doc)
      end
    end
  end
end
