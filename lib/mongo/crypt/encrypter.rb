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

    # A class that implements I/O methods between the driver and
    # the MongoDB server or mongocryptd.
    #
    # This class should have its own file, just leaving it here
    # for simplicity. Should also have a name that is not IO.
    #
    # @api private
    class EncryptionIO
      # Creates a new IO object with information about how to connect
      # to the key vault.
      #
      # @param [ Mongo::Collection ] The key vault collection
      def initialize(key_vault_client, key_vault_namespace)
        key_vault_db_name, key_vault_collection_name = key_vault_namespace.split('.')
        @collection = key_vault_client.use(key_vault_db_name)[key_vault_collection_name]
      end

      # Query for keys in the key vault collection using the provided
      # filter
      #
      # @param [ Hash ] filter
      #
      # @return [ Array<Hash> ] The query results
      def find_keys(filter)
        @collection.find(filter).to_a
      end

      # TODO: documentation
      def insert(document)
        @collection.insert_one(document)
      end
    end

    # TODO: documentation
    class Encrypter
      attr_accessor :encryption_options
      # TODO: documentation
      def set_encryption_options(options = {})
        @encryption_options = options
        @encryption_options.freeze

        validate_key_vault_namespace!
        validate_key_vault_client!

        @crypt_handle = Crypt::Handle.new(options[:kms_providers])
        @io = EncryptionIO.new(options[:key_vault_client], options[:key_vault_namespace])
      end

      private

      # TODO: documentation
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

      # TODO: documentation
      def validate_key_vault_client!
        unless @encryption_options[:key_vault_client]
          raise ArgumentError.new('The :key_vault_client option cannot be nil')
        end
      end
    end
  end
end
