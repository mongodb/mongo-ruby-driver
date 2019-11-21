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

require 'ffi'
require 'base64'

module Mongo
  module Crypt

    # A handle to the libmongocrypt library that wraps a mongocrypt_t object,
    # allowing clients to set options on that object or perform operations such
    # as encryption and decryption
    class Handle

      # Creates a new Handle object and initializes it with options
      #
      # @param [ Hash ] kms_providers A hash of KMS settings. The only supported key
      # is currently :local. Local KMS options must be passed in the format
      # { local: { key: <master key> } } where the master key is a 96-byte, base64
      # encoded string.
      #
      # There will be more arguemnts to this method once automatic encryption is introduced.
      def initialize(kms_providers)
        @mongocrypt = FFI::AutoPointer.new(
          Binding.mongocrypt_new,
          Binding.method(:mongocrypt_destroy)
        )

        set_kms_providers(kms_providers)
        initialize_mongocrypt
      end

      # Return the reference to the underlying @mongocrypt object
      #
      # @return [ FFI::Pointer ]
      def ref
        @mongocrypt
      end

      private

      # Validate the kms_providers option and use it to set the KMS provider
      # information on the underlying mongocrypt_t object
      def set_kms_providers(kms_providers)
        unless kms_providers
          raise ArgumentError.new("The kms_providers option must not be nil")
        end

        unless kms_providers.key?(:local) || kms_providers.key?(:aws)
          raise ArgumentError.new('The kms_providers option must have one of the following keys: :aws, :local')
        end

        set_kms_providers_local(kms_providers) if kms_providers.key?(:local)

        if kms_providers.key?(:aws)
          raise ArgumentError.new(':aws is not yet a supported kms_providers option. Use :local instead')
        end
      end

      # Validate and set the local KMS provider information on the underlying
      # mongocrypt_t object and raise an exception if the operation fails
      def set_kms_providers_local(kms_providers)
        unless kms_providers[:local][:key] && kms_providers[:local][:key].is_a?(String)
          raise ArgumentError.new(
            "The specified kms_providers option is invalid: #{kms_providers}. " +
            "kms_providers with :local key must be in the format: { local: { key: 'MASTER-KEY' } }"
          )
        end

        master_key = kms_providers[:local][:key]

        binary = Binary.new(Base64.decode64(master_key))
        success = Binding.mongocrypt_setopt_kms_provider_local(@mongocrypt, binary.ref)

        raise_from_status unless success
      end

      # Initialize the underlying mongocrypt_t object and raise an error if the operation fails
      def initialize_mongocrypt
        success = Binding.mongocrypt_init(@mongocrypt)
        # There is currently no test for this code path
        raise_from_status unless success
      end

      # Raise a Mongo::Error::CryptError based on the status of the underlying
      # mongocrypt_t object
      def raise_from_status
        status = Status.new

        Binding.mongocrypt_status(@mongocrypt, status.ref)
        status.raise_crypt_error
      end
    end
  end
end
