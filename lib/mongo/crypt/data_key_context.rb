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
      # @param [ String ] kms_provider The KMS provider to use. Options are
      #   "aws", "azure" and "local".
      # @param [ Hash ] options Data key creation options.
      #
      # @option options [ Hash ] :master_key A Hash of options related to the
      #   KMS provider option. Required if kms_provider is "aws" or "azure".
      #   - :region [ String ] The The AWS region of the master key (required).
      #   - :key [ String ] The Amazon Resource Name (ARN) of the master key (required).
      #   - :endpoint [ String ] An alternate host to send KMS requests to (optional).
      # @option options [ Array<String> ] :key_alt_names An optional array of strings specifying
      #   alternate names for the new data key.
      def initialize(mongocrypt, io, key_document, key_alt_names = nil)
        super(mongocrypt, io)
        Binding.ctx_setopt_key_encryption_key(self, key_document.to_document)
        set_key_alt_names(key_alt_names) if key_alt_names
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
