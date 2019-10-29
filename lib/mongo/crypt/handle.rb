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

    # TODO: documentation
    class Handle

      # Creates a new Handle object and initializes it with options
      #
      # @example Instantiate a Handle object with local KMS provider options
      #   Mongo::Crypt::Handle.new({
      #     kms_providers: {
      #       local: { key: 'MASTER-KEY' }
      #     }
      #   })
      #
      # @param [ Hash ] options The options used to initialize the mongocrypt handle
      #
      # @options option [ Hash ] kms_providers A hash of KMS settings. The only supported key
      #   is currently :local. Local KMS options must be passed in the format { local: { key: 'MASTER-KEY' } }
      #   where the master key is a 96-byte, base64 encoded string.
      #
      # @since 2.12.0
      def initialize(options = {})
        raise ArgumentError.new("Options must not be blank") unless options

        @options = options
        @mongocrypt = Binding.mongocrypt_new

        begin
          set_kms_providers
          initialize_mongocrypt
        rescue => e
          self.close
          raise e
        end
      end


      # TODO: documentation
      def close
        Binding.mongocrypt_destroy(@mongocrypt) if @mongocrypt
        @status.close if @status

        @mongocrypt = nil
        @status = nil
        @options = nil

        true
      end

      private

      # Validate the kms_providers option and use it to set the KMS provider
      # information on the underlying mongocrypt_t object
      def set_kms_providers
        unless @options[:kms_providers]
          raise ArgumentError.new("The kms_providers option must not be blank")
        end

        kms_providers = @options[:kms_providers]

        unless kms_providers.key?(:local) || kms_providers.key?(:aws)
          raise ArgumentError.new('The kms_providers option must have one of the following keys: :aws, :local')
        end

        if kms_providers.key?(:local)
          unless kms_providers[:local][:key] && kms_providers[:local][:key].is_a?(String)
            raise ArgumentError.new(
              "The specified kms_providers option is invalid: #{kms_providers}. " +
              "kms_providers with :local key must be in the format: { local: { key: 'MASTER-KEY' } }"
            )
          end

          set_kms_provider_local
        end

        if kms_providers.key?(:aws)
          raise ArgumentError.new(':aws is not yet a supported kms_providers option. Use :local instead')
        end
      end

      # Set the local KMS provider information on the underlying mongocrypt_t object
      #
      # Only called once it has been validated that @options[:kms_providers][:local][:key]
      # is present and a String
      #
      # Raises an error if the operation fails
      def set_kms_provider_local
        master_key = @options[:kms_providers][:local][:key]

        Binary.with_binary(Base64.decode64(master_key)) do |binary|
          success = Binding.mongocrypt_setopt_kms_provider_local(@mongocrypt, binary.ref)
          raise_from_status unless success
        end
      end

      # Initialize the underlying mongocrypt_t object and raise an error if the operation fails
      def initialize_mongocrypt
        success = Binding.mongocrypt_init(@mongocrypt)
        raise_from_status unless success
      end

      # Raise a Mongo::Error::CryptError based on the status of the underlying
      # mongocrypt_t object
      def raise_from_status
        Status.with_status do |status|
          Binding.mongocrypt_status(@mongocrypt, status.ref)

          message = "Code #{status.code}: #{status.message}"

          error = case status.label
          when :error_kms
            # There is currently no test for this code path
            Error::CryptKmsError.new(status.code, message)
          when :error_client
            Error::CryptClientError.new(status.code, message)
          end

          raise error
        end
      end
    end
  end
end
