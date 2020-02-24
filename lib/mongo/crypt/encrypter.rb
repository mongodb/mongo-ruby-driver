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

    # A module that encapsulates client-side encryption functionality. This class
    # can is extended to provide specific functionality for explicit and
    # auto-encryption.
    #
    # @api private
    class Encrypter
      attr_reader :encryption_options
      attr_reader :crypt_handle
      attr_reader :encryption_io

      # Validate that the key_vault_namespace option is not nil and
      # is in the format database.collection
      private def validate_key_vault_namespace!
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
      private def validate_key_vault_client!
        unless @encryption_options[:key_vault_client]
          raise ArgumentError.new('The :key_vault_client option cannot be nil')
        end
      end

      # Build the key vault collection for EncryptionIO object
      private def build_key_vault_collection
        key_vault_db, key_vault_coll = @encryption_options[:key_vault_namespace].split('.')
        @encryption_options[:key_vault_client].use(key_vault_db)[key_vault_coll]
      end
    end
  end
end
