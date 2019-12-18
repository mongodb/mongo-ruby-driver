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
        # FFI::AutoPointer uses a custom release strategy to automatically free
        # the pointer once this object goes out of scope
        @mongocrypt = FFI::AutoPointer.new(
          Binding.mongocrypt_new,
          Binding.method(:mongocrypt_destroy)
        )

        set_logger
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

      # Send the logs from libmongocrypt to the Mongo::Logger
      def set_logger
        @log_callback = Proc.new do |level, msg|
          logger = Mongo::Logger.logger

          case level
          when :fatal
            logger.fatal(msg)
          when :error
            logger.error(msg)
          when :warning
            logger.warn(msg)
          when :info
            logger.info(msg)
          when :trace
            logger.debug(msg)
          else
            raise ArgumentError.new("Invalid log level: #{level}")
          end
        end

        success = Binding.mongocrypt_setopt_log_handler(@mongocrypt, @log_callback, nil)
        raise_from_status unless success
      end

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
        set_kms_providers_aws(kms_providers) if kms_providers.key?(:aws)
      end

      # Validate and set the local KMS provider information on the underlying
      # mongocrypt_t object and raise an exception if the operation fails
      def set_kms_providers_local(kms_providers)
        unless kms_providers[:local][:key] && kms_providers[:local][:key].is_a?(String)
          raise ArgumentError.new(
            "The specified local kms_providers option is invalid: #{kms_providers[:local]}. " +
            "kms_providers with :local key must be in the format: { local: { key: 'MASTER-KEY' } }"
          )
        end

        master_key = kms_providers[:local][:key]

        binary = Binary.new(Base64.decode64(master_key))
        success = Binding.mongocrypt_setopt_kms_provider_local(@mongocrypt, binary.ref)

        raise_from_status unless success
      end

      # Validate and set the aws KMS provider information on the underlying
      # mongocrypt_t object and raise an exception if the operation fails
      def set_kms_providers_aws(kms_providers)
        access_key_id = kms_providers[:aws][:access_key_id]
        secret_access_key = kms_providers[:aws][:secret_access_key]

        unless access_key_id && access_key_id.is_a?(String) && secret_access_key && secret_access_key.is_a?(String)
          raise ArgumentError.new(
            "The specified aws kms_providers option is invalid: #{kms_providers[:aws]}. " +
            "kms_providers with :aws key must be in the format: { aws: { access_key_id: 'YOUR-ACCESS-KEY-ID', secret_access_key: 'SECRET-ACCESS-KEY' } }"
          )
        end

        # TODO: Set the AWS kms provider on the underlying mongocrypt_t object
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
