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

    # Wraps a libmongocrypt mongocrypt_kms_ctx_t object. Contains information
    # about making an HTTP request to fetch information about a KMS
    # data key.
    class KmsContext
      # Create a new KmsContext object.
      #
      # @param [ FFI::Pointer ] kms_ctx A pointer to a mongocrypt_kms_ctx_t
      #   object. This object is managed by the mongocrypt_ctx_t object that
      #   created it; this class is not responsible for de-allocating resources.
      def initialize(kms_ctx)
        @kms_ctx_p = kms_ctx
      end

      # Return the pointer to the underlying mongocrypt_kms_ctx_t object.
      #
      # @return [ FFI::Pointer ] A pointer to a mongocrypt_kms_ctx_t object.
      attr_reader :kms_ctx_p

      # Return the endpoint at which to make the HTTP request.
      #
      # @return [ String ] The endpoint.
      def endpoint
        Binding.kms_ctx_endpoint(self)
      end

      # Return the HTTP message to send to fetch information about the relevant
      # KMS data key.
      #
      # @return [ String ] The HTTP message.
      def message
        Binding.kms_ctx_message(self)
      end

      # Return the number of bytes still needed by libmongocrypt to complete
      # the request for information about the AWS data key.
      #
      # @return [ Integer ] The number of bytes needed.
      def bytes_needed
        Binding.kms_ctx_bytes_needed(self)
      end

      # Feed a response from the HTTP request to libmongocrypt.
      #
      # @param [ String ] data Data to feed to libmongocrypt.
      def feed(data)
        Binding.kms_ctx_feed(self, data)
      end
    end
  end
end
