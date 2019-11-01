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

    # A wrapper around mongocrypt_ctx_t, which manages the
    # state machine for encryption and decription.
    #
    # This class is a superclass that defines shared methods
    # amongst contexts that are initialized for different purposes
    # (e.g. data key creation, encryption, explicit encryption, etc.)
    class Context
      #  Create a new Context object
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_t object
      #   used to create a new mongocrypt_ctx_t
      def initialize(mongocrypt)
        @ctx = Binding.mongocrypt_ctx_new(mongocrypt)
      end

      # Releases allocated memory and cleans up resources
      #
      # @return [ true ] Always true
      def close
        Binding.mongocrypt_ctx_destroy(@ctx) if @ctx
        @ctx = nil

        true
      end

      private

      # Raise a Mongo::Error::CryptError based on the status of the underlying
      # mongocrypt_ctx_t object
      def raise_from_status
        Status.with_status do |status|
          Binding.mongocrypt_ctx_status(@ctx, status.ref)
          status.raise_crypt_error
        end
      end
    end
  end
end
