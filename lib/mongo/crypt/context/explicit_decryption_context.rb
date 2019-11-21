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

    # A Context object initialized for explicit decryption
    class ExplicitDecryptionContext < Context

      # Create a new ExplicitDecryptionContext object
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_t object
      #   used to create a new mongocrypt_ctx_t
      # @param [ ClientEncryption::IO ] A instance of the IO class
      #   that implements driver I/O methods required to run the
      #   state machine
      # @param [ String ] value A BSON value to decrypt
      def initialize(mongocrypt, io, value)
        super(mongocrypt, io)

        @value = value

        begin
          initialize_ctx
        rescue => e
          # Initializing the context could raise errors.
          # Make sure the reference to the underlying mongocrypt_ctx_t is destroyed
          # before passing those errors along.
          self.close
          raise e
        end
      end

      # Convenient API for using context object without having
      # to perform cleanup.
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_t object
      #   used to create a new mongocrypt_ctx_t
      # @param [ ClientEncryption::IO ] A instance of the IO class
      #   that implements driver I/O methods required to run the
      #   state machine
      # @param [ String ] value A BSON value to decrypt
      def self.with_context(mongocrypt, io, value)
        context = self.new(mongocrypt, io, value)
        begin
          yield(context)
        ensure
          context.close
        end
      end

      private

      # Initialize the underlying mongocrypt_ctx_t object to perform
      # explicit decryption
      def initialize_ctx
        binary = Binary.new(@value)
        success = Binding.mongocrypt_ctx_explicit_decrypt_init(@ctx, binary.ref)

        raise_from_status unless success
      end
    end
  end
end
