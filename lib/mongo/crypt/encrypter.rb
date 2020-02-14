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

    # A module that encapsulates client-side-encryption functionality
    #
    # @api private
    module Encrypter
      attr_reader :encryption_options

      # Set up encryption-related options and instance variables
      # on the class that includes this module.
      #
      # @param options [ Hash ] options
      #
      # @option options [ Mongo::Client ] :key_vault_client A client connected
      #   to the MongoDB instance containing the encryption key vault.
      # @option options [ String ] :key_vault_namespace The namespace of the key
      #   vault in the format database.collection.
      #
      # @raise [ ArgumentError ] If required options are missing or incorrectly
      #   formatted.
      def setup_encrypter(options = {})
        @encryption_options = options.dup.freeze

        validate_key_vault_namespace!
        validate_key_vault_client!

        @crypt_handle = Crypt::Handle.new(options[:kms_providers], schema_map: options[:schema_map])
        @encryption_io = EncryptionIO.new(key_vault_collection: build_key_vault_collection)
      end

      private

      # Validate that the key_vault_namespace option is not nil and
      # is in the format database.collection
      def validate_key_vault_namespace!
        key_vault_namespace = @encryption_options[:key_vault_namespace]

        unless key_vault_namespace
          raise ArgumentError.new('The :key_vault_namespace option cannot be nil')
        end

        unless key_vault_namespace.split('.').length == 2
          raise ArgumentError.new(
            "#{key_vault_namespace} is an invalid key vault namespace." +
            "The :key_vault_namespace option must be in the format database.collection"
          )
        end
      end

      # Validate that the key_vault_client option is not nil
      def validate_key_vault_client!
        unless @encryption_options[:key_vault_client]
          raise ArgumentError.new('The :key_vault_client option cannot be nil')
        end
      end

      # Build the key vault collection for EncryptionIO object
      def build_key_vault_collection
        key_vault_db, key_vault_coll = @encryption_options[:key_vault_namespace].split('.')
        @encryption_options[:key_vault_client].use(key_vault_db)[key_vault_coll]
      end
    end
  end
end
