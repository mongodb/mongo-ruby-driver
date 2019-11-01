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
    class Context
      # TODO: documentation
      def initialize(ctx)
        @ctx = ctx
      end

      # TODO: documentation
      def close
        Binding.mongocrypt_ctx_destroy(@ctx) if @ctx
        @ctx = nil
      end

      private

      # Raise a Mongo::Error::CryptError based on the status of the underlying
      # mongocrypt_ctx_t object
      def raise_from_status
        Status.with_status do |status|
          Binding.mongocrypt_ctx_status(@ctx, status.ref)

          error = case status.label
          when :error_kms
            # There is currently no test for this code path
            Error::CryptKmsError.new(status.code, status.message)
          when :error_client
            Error::CryptClientError.new(status.code, status.message)
          end

          raise error
        end
      end
    end
  end
end
