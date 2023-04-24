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
  module Crypt

    # A Context object initialized specifically for the purpose of creating
    # a data key in the key management system.
    #
    # @api private
    class DataKeyContext < Context

      # Create a new DataKeyContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      # @param [ Mongo::Crypt::EncryptionIO ] io An object that performs all
      #   driver I/O on behalf of libmongocrypt
      # @param [ Mongo::Crypt::KMS::MasterKeyDocument ] master_key_document The master
      #   key document that contains master encryption key parameters.
      # @param [ Array<String> | nil ] key_alt_names An optional array of strings specifying
      #   alternate names for the new data key.
      # @param [ String | nil ] :key_material Optional
      #   96 bytes to use as custom key material for the data key being created.
      #   If :key_material option is given, the custom key material is used
      #   for encrypting and decrypting data.
      def initialize(mongocrypt, io, master_key_document, key_alt_names, key_material)
        super(mongocrypt, io)
        Binding.ctx_setopt_key_encryption_key(self, master_key_document.to_document)
        set_key_alt_names(key_alt_names) if key_alt_names
        Binding.ctx_setopt_key_material(self, BSON::Binary.new(key_material)) if key_material
        initialize_ctx
      end

      private

      # Set the alt names option on the context
      def set_key_alt_names(key_alt_names)
        unless key_alt_names.is_a?(Array)
          raise ArgumentError.new, 'The :key_alt_names option must be an Array'
        end

        unless key_alt_names.all? { |key_alt_name| key_alt_name.is_a?(String) }
          raise ArgumentError.new(
            "#{key_alt_names} contains an invalid alternate key name. All " +
            "values of the :key_alt_names option Array must be Strings"
          )
        end

        Binding.ctx_setopt_key_alt_names(self, key_alt_names)
      end

      # Initializes the underlying mongocrypt_ctx_t object
      def initialize_ctx
        Binding.ctx_datakey_init(self)
      end
    end
  end
end
