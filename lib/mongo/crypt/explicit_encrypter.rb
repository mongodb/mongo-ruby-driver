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

    # TODO: documentation
    #
    # @api private
    class ExplicitEncrypter < Encrypter
      # TODO: documentation
      def initialize(options={})
        @encryption_options = options.dup.freeze

        validate_key_vault_namespace!
        validate_key_vault_client!

        @encryption_io = EncryptionIO.new(key_vault_collection: build_key_vault_collection)
        @crypt_handle = Handle.new(options[:kms_providers])
      end

      def create_data_key(kms_provider, options={})
      end

      def encrypt(value, options={})

      end

      def decrypt(value)

      end
    end
  end
end
