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

    # A Context object initialized specifically for the purpose of rewrapping
    # data keys (decrypting and re-rencryting using a new KEK).
    #
    # @api private
    class RewrapManyDataKeyContext < Context

      # Create a new RewrapManyDataKeyContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      # @param [ Mongo::Crypt::EncryptionIO ] io An object that performs all
      #   driver I/O on behalf of libmongocrypt
      # @param [ Hash ] filter Filter used to find keys to be updated.
      #   alternate names for the new data key.
      # @param [ Mongo::Crypt::KMS::MasterKeyDocument | nil ] master_key_document The optional master
      #   key document that contains master encryption key parameters.
      def initialize(mongocrypt, io, filter, master_key_document)
        super(mongocrypt, io)
        if master_key_document
          Binding.ctx_setopt_key_encryption_key(self, master_key_document.to_document)
        end
        Binding.ctx_rewrap_many_datakey_init(self, filter)
      end
    end
  end
end
