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
    #
    # @api private
    class Handle
      # Creates a new Handle object and initializes it with options
      #
      # @param [ Hash ] kms_providers A hash of KMS settings. The only supported key
      #   is currently :local. Local KMS options must be passed in the format
      #   { local: { key: <master key> } } where the master key is a 96-byte, base64
      #   encoded string.
      # @param [ Hash ] options A hash of options
      #
      # @option [ Hash | nil ] :schema_map A hash representing the JSON schema of the collection
      #   that stores auto encrypted documents.
      # @option [ Logger ] :logger A Logger object to which libmongocrypt logs
      #   will be sent
      #
      # There will be more arguemnts to this method once automatic encryption is introduced.
      def initialize(kms_providers, options={})
        # FFI::AutoPointer uses a custom release strategy to automatically free
        # the pointer once this object goes out of scope
        @mongocrypt = FFI::AutoPointer.new(
          Binding.mongocrypt_new,
          Binding.method(:mongocrypt_destroy)
        )

        @schema_map = options[:schema_map]
        set_schema_map if @schema_map

        @logger = options[:logger]
        set_logger_callback if @logger

        set_crypto_hooks

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

      # Set the schema map option on the underlying mongocrypt_t object
      def set_schema_map
        unless @schema_map.is_a?(Hash)
          raise ArgumentError.new("#{@schema_map} is an invalid schema_map; schema_map must be a Hash or nil")
        end

        binary = Binary.from_data(@schema_map.to_bson.to_s)
        success = Binding.mongocrypt_setopt_schema_map(@mongocrypt, binary.ref)

        raise_from_status unless success
      end

      # Send the logs from libmongocrypt to the Mongo::Logger
      def set_logger_callback
        @log_callback = Proc.new do |level, msg|
          @logger.send(level, msg)
        end

        success = Binding.mongocrypt_setopt_log_handler(@mongocrypt, @log_callback, nil)
        raise_from_status unless success
      end

      # In the case of an error during cryptography, set the error message
      # of the specified status to the message of the runtime error
      def handle_error(status_p, e)
        status = Status.from_pointer(status_p)
        status.update(:error_client, 1, "#{e.class}: #{e}")
      end

      def do_aes(key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p, decrypt: false)
        key = Binary.from_pointer(key_binary_p).to_string
        iv = Binary.from_pointer(iv_binary_p).to_string
        input = Binary.from_pointer(input_binary_p).to_string

        begin
          output = Hooks.aes(key, iv, input, decrypt: decrypt)

          Binary.from_pointer(output_binary_p).write(output)

          response_length_p.write_int(output.length)
          true
        rescue => e
          handle_error(status_p, e)
          false
        end
      end

      def aes_encrypt(_, key_binary_p, iv_binary_p, input_binary_p,
        output_binary_p, response_length_p, status_p
      )
        do_aes(key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p)
      end

      def aes_decrypt(_, key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p)
        do_aes(key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p, decrypt: true)
      end

      # We are buildling libmongocrypt without crypto functions to remove the
      # external dependency on OpenSSL. This method binds native Ruby crypto methods
      # to the underlying mongocrypt_t object so that libmongocrypt can still perform
      # cryptography.
      #
      # Every crypto binding ignores its first argument, which is an option mongocrypt_ctx_t
      # object and is not required to use crypto hooks.
      def set_crypto_hooks

        @random_fn = Proc.new do |_, output_binary_p, num_bytes, status_p|
          Hooks.random(output_binary_p, num_bytes, status_p)
        end

        @hmac_sha_512_fn = Proc.new do |_, key_binary_p, input_binary_p, output_binary_p, status_p|
          Hooks.hmac_sha('SHA512', key_binary_p, input_binary_p, output_binary_p, status_p)
        end

        @hmac_sha_256_fn = Proc.new do |_, key_binary_p, input_binary_p, output_binary_p, status_p|
          Hooks.hmac_sha('SHA256', key_binary_p, input_binary_p, output_binary_p, status_p)
        end

        @hmac_hash_fn = Proc.new do |_, input_binary_p, output_binary_p, status_p|
          Hooks.hash_sha256(input_binary_p, output_binary_p, status_p)
        end

        success = Binding.mongocrypt_setopt_crypto_hooks(
                    @mongocrypt,
                    method(:aes_encrypt),
                    method(:aes_decrypt),
                    @random_fn,
                    @hmac_sha_512_fn,
                    @hmac_sha_256_fn,
                    @hmac_hash_fn,
                    nil
                  )

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

        binary = Binary.from_data(Base64.decode64(master_key))
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
