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

    # A Context object initialized specifically for the purpose of creating
    # a data key in the key managemenet system.
    #
    # @api private
    class DataKeyContext < Context

      # Create a new DataKeyContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      def initialize(mongocrypt)
        # This initializer will eventually take more arguments:
        # - kms_providers (just supporting local for right now)
        # - options: master key (only relevant to AWS) and key_alt_names (not required for POC)

        super(mongocrypt, nil)

        set_local_master_key
        initialize_ctx
      end

      private

      # Configures the underlying mongocrypt_ctx_t object to accept local
      # KMS options
      def set_local_master_key
        success = Binding.mongocrypt_ctx_setopt_masterkey_local(@ctx)
        raise_from_status unless success
      end

      # Initializes the underlying mongocrypt_ctx_t object
      def initialize_ctx
        success = Binding.mongocrypt_ctx_datakey_init(@ctx)
        raise_from_status unless success
      end
    end
  end
end
