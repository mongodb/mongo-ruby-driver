# Copyright (C) 2020 MongoDB, Inc.
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

    # An ExplicitEncrypter is an object that performs explicit encryption
    # operations and handles all associated options and instance variables.
    #
    # @api private
    class ExplicitEncrypter
      # TODO: documentation
      def initialize(options)
        @options = options.dup.freeze

        @encryption_io = EncryptionIO.new(
          key_vault_namespace: @options[:key_vault_namespace],
          key_vault_client: @options[:key_vault_client]
        )
        @crypt_handle = Handle.new(@options[:kms_providers])
      end

      def create_and_insert_data_key(kms_provider, options)
        data_key_document = Crypt::DataKeyContext.new(
          @crypt_handle,
          @encryption_io,
          kms_provider,
          options
        ).run_state_machine

        @encryption_io.insert(data_key_document)
      end

      def encrypt(doc, options)
        Crypt::ExplicitEncryptionContext.new(
          @crypt_handle,
          @encryption_io,
          doc,
          options
        ).run_state_machine
      end

      def decrypt(doc)
        result = Crypt::ExplicitDecryptionContext.new(
          @crypt_handle,
          @encryption_io,
          doc,
        ).run_state_machine
      end
    end
  end
end
