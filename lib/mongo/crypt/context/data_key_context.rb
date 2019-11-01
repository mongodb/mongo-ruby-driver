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

# require 'ffi'

module Mongo
  module Crypt

    # TODO: documentation
    class DataKeyContext < Context

      # TODO: documentation
      def initialize(ctx)
        # This initializer will eventually take more arguments:
        # - kms_providers (just supporting local for right now)
        # - options: master key (only relevant to AWS) and key_alt_names (not required for POC)

        super(ctx)

        begin
          set_local_master_key
          initialize_ctx
        rescue => e
          self.close
          raise e
        end
      end

      # TODO: documentation
      def self.with_context(ctx)
        context = self.new(ctx)
        begin
          yield(context)
        ensure
          context.close
        end
      end

      private

      def set_local_master_key
        success = Binding.mongocrypt_ctx_setopt_masterkey_local(@ctx)
        raise_from_status unless success
      end

      def initialize_ctx
        success = Binding.mongocrypt_ctx_datakey_init(@ctx)
        raise_from_status unless success
      end
    end
  end
end
