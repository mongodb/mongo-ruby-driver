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

require 'byebug' # TODO: remove

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
        # Ideally, this level of the API wouldn't be passing around pointer
        # references between objects, so this method signature is subject to change.
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
            filter = mongo_operation
            @io.find_keys(filter).each do |key|
              mongo_feed(key) if key
            end

            Binding.mongocrypt_ctx_mongo_done(@ctx)
          else
            # There are three other states to handle:
            # - :need_mongo_collinfo
            # - :need_mongo_markings
            # - :need_kms
            #
            # None of these are required to create data keys,
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
        Status.with_status do |status|
          Binding.mongocrypt_ctx_status(@ctx, status.ref)
          status.raise_crypt_error
        end
      end

      # Finalize the state machine and return the result as a string
      def finalize_state_machine
        Binary.with_binary do |binary|
          success = Binding.mongocrypt_ctx_finalize(@ctx, binary.ref)
          raise_from_status unless success
          return binary.to_string
        end
      end

      # TODO: documentation
      def mongo_operation
        Binary.with_binary do |binary|
          success = Binding.mongocrypt_ctx_mongo_op(@ctx, binary.ref)
          raise_from_status unless success
          return binary.to_string
          # return BSON::Binary.new(binary.to_string)
        end
      end

      # TODO: documentation
      def mongo_feed(result)
        result = result.to_bson.to_s
        Binary.with_binary(result) do |binary|
          success = Binding.mongocrypt_ctx_mongo_feed(@ctx, binary.ref)
          raise_from_status unless success
          return binary.to_string
        end
      end
    end
  end
end
