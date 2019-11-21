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
      # @param [ ClientEncryption::IO ] A instance of the IO class
      #   that implements driver I/O methods required to run the
      #   state machine
      def initialize(mongocrypt, io)
        # Ideally, this level of the API wouldn't be passing around pointer
        # references between objects, so this method signature is subject to change.

        # FFI::AutoPointer uses a custom release strategy to automatically free
        # the pointer once this object goes out of scope
        @ctx = FFI::AutoPointer.new(
          Binding.mongocrypt_ctx_new(mongocrypt),
          Binding.method(:mongocrypt_ctx_destroy)
        )

        @io = io
      end

      # Returns the state of the mongocrypt_ctx_t
      #
      # @return [ Symbol ] The context state
      def state
        Binding.mongocrypt_ctx_state(@ctx)
      end

      # Runs the mongocrypt_ctx_t state machine and handles
      # all I/O on behalf of libmongocrypt
      #
      # @return [ String|nil ] A BSON string representing the outcome
      #   of the state machine. This string could represent different
      #   values depending on how the context was initialized.
      #
      # @raise [ Error::CryptError ] If the state machine enters the
      #   :error state
      #
      # This method is not currently unit tested. It is integration tested
      # in spec/integration/explicit_encryption_spec.rb
      def run_state_machine
        while true
          case state
          when :error
            raise_from_status
          when :ready
            return finalize_state_machine
          when :done
            return nil
          when :need_mongo_keys
            filter = Hash.from_bson(BSON::ByteBuffer.new(mongo_operation))

            @io.find_keys(filter).each do |key|
              mongo_feed(key.to_bson.to_s) if key
            end

            Binding.mongocrypt_ctx_mongo_done(@ctx)
          else
            # There are three other states to handle:
            # - :need_mongo_collinfo
            # - :need_mongo_markings
            # - :need_kms
            #
            # None of these are required for explicit encryption/decryption,
            # so these parts of the state machine will be implemented
            # later
            raise("State #{state} is not yet supported by Mongo::Crypt::Context")
          end
        end
      end

      private

      # Raise a Mongo::Error::CryptError based on the status of the underlying
      # mongocrypt_ctx_t object
      def raise_from_status
        status = Status.new

        Binding.mongocrypt_ctx_status(@ctx, status.ref)
        status.raise_crypt_error
      end

      # Finalize the state machine and return the result as a string
      def finalize_state_machine
        binary = Binary.new
        success = Binding.mongocrypt_ctx_finalize(@ctx, binary.ref)
        raise_from_status unless success

        binary.to_string
      end

      # Returns a binary string representing a mongo operation that the
      # driver must perform to get the information it needs in order to
      # continue with encryption/decryption (for example, a filter for
      # a key vault query).
      def mongo_operation
        binary = Binary.new
        success = Binding.mongocrypt_ctx_mongo_op(@ctx, binary.ref)
        raise_from_status unless success

        binary.to_string
      end

      # Feeds the result of a Mongo operation to the underlying mongocrypt_ctx_t
      # object. The result param should be a binary string.
      def mongo_feed(result)
        binary = Binary.new(result)
        success = Binding.mongocrypt_ctx_mongo_feed(@ctx, binary.ref)

        raise_from_status unless success
      end
    end
  end
end
