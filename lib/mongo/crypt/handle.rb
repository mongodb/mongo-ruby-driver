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

      # @returns [ Crypt::KMS::Credentials ] Credentials for KMS providers.
      attr_reader :kms_providers

      # Creates a new Handle object and initializes it with options
      #
      # @param [ Crypt::KMS::Credentials ] kms_providers Credentials for KMS providers.
      #
      # @param [ Hash ] kms_tls_options TLS options to connect to KMS
      #   providers. Keys of the hash should be KSM provider names; values
      #   should be hashes of TLS connection options. The options are equivalent
      #   to TLS connection options of Mongo::Client.
      #
      # @param [ Hash ] options A hash of options.
      # @option options [ Hash | nil ] :schema_map A hash representing the JSON schema
      #   of the collection that stores auto encrypted documents. This option is
      #   mutually exclusive with :schema_map_path.
      # @option options [ String | nil ] :schema_map_path A path to a file contains the JSON schema
      #   of the collection that stores auto encrypted documents. This option is
      #   mutually exclusive with :schema_map.
      # @option options [ Hash | nil ] :encrypted_fields_map maps a collection
      #   namespace to an encryptedFields.
      #   - Note: If a collection is present on both the encryptedFieldsMap
      #     and schemaMap, an error will be raised.
      # @option options [ Boolean | nil ] :bypass_query_analysis When true
      #   disables automatic analysis of outgoing commands.
      # @option options [ String | nil ] :crypt_shared_lib_path Path that should
      #   be  the used to load the crypt shared library. Providing this option
      #   overrides default crypt shared library load paths for libmongocrypt.
      # @option options [ Boolean | nil ] :crypt_shared_lib_required Whether
      #   crypt_shared library is required. If 'true', an error will be raised
      #   if a crypt_shared library cannot be loaded by libmongocrypt.
      # @option options [ Boolean | nil ] :explicit_encryption_only Whether this
      #   handle is going to be used only for explicit encryption. If true,
      #   libmongocrypt is instructed not to load crypt shared library.
      # @option options [ Logger ] :logger A Logger object to which libmongocrypt logs
      #   will be sent
      def initialize(kms_providers, kms_tls_options, options={})
        # FFI::AutoPointer uses a custom release strategy to automatically free
        # the pointer once this object goes out of scope
        @mongocrypt = FFI::AutoPointer.new(
          Binding.mongocrypt_new,
          Binding.method(:mongocrypt_destroy)
        )

        @kms_providers = kms_providers
        @kms_tls_options =  kms_tls_options

        maybe_set_schema_map(options)

        @encrypted_fields_map = options[:encrypted_fields_map]
        set_encrypted_fields_map if @encrypted_fields_map

        @bypass_query_analysis = options[:bypass_query_analysis]
        set_bypass_query_analysis if @bypass_query_analysis

        @crypt_shared_lib_path = options[:crypt_shared_lib_path]
        @explicit_encryption_only = options[:explicit_encryption_only]
        if @crypt_shared_lib_path
          Binding.setopt_set_crypt_shared_lib_path_override(self, @crypt_shared_lib_path)
        elsif !@bypass_query_analysis && !@explicit_encryption_only
          Binding.setopt_append_crypt_shared_lib_search_path(self, "$SYSTEM")
        end

        @logger = options[:logger]
        set_logger_callback if @logger

        set_crypto_hooks

        Binding.setopt_kms_providers(self, @kms_providers.to_document)

        if @kms_providers.aws&.empty? || @kms_providers.gcp&.empty? || @kms_providers.azure&.empty?
          Binding.setopt_use_need_kms_credentials_state(self)
        end

        initialize_mongocrypt

        @crypt_shared_lib_required = !!options[:crypt_shared_lib_required]
        if @crypt_shared_lib_required && crypt_shared_lib_version == 0
          raise Mongo::Error::CryptError.new(
            "Crypt shared library is required, but cannot be loaded  according to libmongocrypt"
          )
        end
      end

      # Return the reference to the underlying @mongocrypt object
      #
      # @return [ FFI::Pointer ]
      def ref
        @mongocrypt
      end

      # Return TLS options for KMS provider. If there are no TLS options set,
      # empty hash is returned.
      #
      # @param [ String ] provider KSM provider name.
      #
      # @return [ Hash ] TLS options to connect to KMS provider.
      def kms_tls_options(provider)
        @kms_tls_options.fetch(provider, {})
      end

      def crypt_shared_lib_version
        Binding.crypt_shared_lib_version(self)
      end

      def crypt_shared_lib_available?
        crypt_shared_lib_version != 0
      end

      private

      # Set the schema map option on the underlying mongocrypt_t object
      def maybe_set_schema_map(options)
        if !options[:schema_map] && !options[:schema_map_path]
          @schema_map = nil
        elsif options[:schema_map] && options[:schema_map_path]
          raise ArgumentError.new(
            "Cannot set both schema_map and schema_map_path options."
          )
        elsif options[:schema_map]
          unless options[:schema_map].is_a?(Hash)
            raise ArgumentError.new(
              "#{@schema_map} is an invalid schema_map; schema_map must be a Hash or nil."
            )
          end
          @schema_map = options[:schema_map]
          Binding.setopt_schema_map(self, @schema_map)
        elsif options[:schema_map_path]
          @schema_map = BSON::ExtJSON.parse(File.read(options[:schema_map_path]))
          Binding.setopt_schema_map(self, @schema_map)
        end
      rescue Errno::ENOENT
        raise ArgumentError.new(
          "#{@schema_map_path} is an invalid path to a file contains schema_map."
        )
      end

      def set_encrypted_fields_map
        unless @encrypted_fields_map.is_a?(Hash)
          raise ArgumentError.new(
            "#{@encrypted_fields_map} is an invalid encrypted_fields_map: must be a Hash or nil"
          )
        end

        Binding.setopt_encrypted_field_config_map(self, @encrypted_fields_map)
      end

      def set_bypass_query_analysis
        unless [true, false].include?(@bypass_query_analysis)
          raise ArgumentError.new(
            "#{@bypass_query_analysis} is an invalid bypass_query_analysis value; must be a Boolean or nil"
          )
        end

        Binding.setopt_bypass_query_analysis(self) if @bypass_query_analysis
      end

      # Send the logs from libmongocrypt to the Mongo::Logger
      def set_logger_callback
        @log_callback = Proc.new do |level, msg|
          @logger.send(level, msg)
        end

        Binding.setopt_log_handler(@mongocrypt, @log_callback)
      end

      # Yields to the provided block and rescues exceptions raised by
      # the block. If an exception was raised, sets the specified status
      # to the exception message and returns false. If no exceptions were
      # raised, does not modify the status and returns true.
      #
      # This method is meant to be used with libmongocrypt callbacks and
      # follows the API defined by libmongocrypt.
      #
      # @param [ FFI::Pointer ] status_p A pointer to libmongocrypt status object
      #
      # @return [ true | false ] Whether block executed without raising
      #   exceptions.
      def handle_error(status_p)
        begin
          yield

          true
        rescue => e
          status = Status.from_pointer(status_p)
          status.update(:error_client, 1, "#{e.class}: #{e}")
          false
        end
      end

      # Yields to the provided block and writes the return value of block
      # to the specified mongocrypt_binary_t object. If an exception is
      # raised during execution of the block, writes the exception message
      # to the specified status object and returns false. If no exception is
      # raised, does not modify status and returns true.
      # message to the mongocrypt_status_t object.
      #
      # @param [ FFI::Pointer ] output_binary_p A pointer to libmongocrypt
      #   Binary object to receive the result of block's execution
      # @param [ FFI::Pointer ] status_p A pointer to libmongocrypt status object
      #
      # @return [ true | false ] Whether block executed without raising
      #   exceptions.
      def write_binary_string_and_set_status(output_binary_p, status_p)
        handle_error(status_p) do
          output = yield

          Binary.from_pointer(output_binary_p).write(output)
        end
      end

      # Perform AES encryption or decryption and write the output to the
      # provided mongocrypt_binary_t object.
      def do_aes(key_binary_p, iv_binary_p, input_binary_p, output_binary_p,
        response_length_p, status_p, decrypt: false, mode: :CBC)
        key = Binary.from_pointer(key_binary_p).to_s
        iv = Binary.from_pointer(iv_binary_p).to_s
        input = Binary.from_pointer(input_binary_p).to_s

        write_binary_string_and_set_status(output_binary_p, status_p) do
          output = Hooks.aes(key, iv, input, decrypt: decrypt, mode: mode)
          response_length_p.write_int(output.bytesize)

          output
        end
      end

      # Perform HMAC SHA encryption and write the output to the provided
      # mongocrypt_binary_t object.
      def do_hmac_sha(digest_name, key_binary_p, input_binary_p,
        output_binary_p, status_p)
        key = Binary.from_pointer(key_binary_p).to_s
        input = Binary.from_pointer(input_binary_p).to_s

        write_binary_string_and_set_status(output_binary_p, status_p) do
          Hooks.hmac_sha(digest_name, key, input)
        end
      end

      # Perform signing using RSASSA-PKCS1-v1_5 with SHA256 hash and write
      # the output to the provided mongocrypt_binary_t object.
      def do_rsaes_pkcs_signature(key_binary_p, input_binary_p,
        output_binary_p, status_p)
        key = Binary.from_pointer(key_binary_p).to_s
        input = Binary.from_pointer(input_binary_p).to_s

        write_binary_string_and_set_status(output_binary_p, status_p) do
          Hooks.rsaes_pkcs_signature(key, input)
        end
      end

      # We are building libmongocrypt without crypto functions to remove the
      # external dependency on OpenSSL. This method binds native Ruby crypto
      # methods to the underlying mongocrypt_t object so that libmongocrypt can
      # still perform cryptography.
      #
      # Every crypto binding ignores its first argument, which is an option
      # mongocrypt_ctx_t object and is not required to use crypto hooks.
      def set_crypto_hooks
        @aes_encrypt = Proc.new do |_, key_binary_p, iv_binary_p, input_binary_p,
          output_binary_p, response_length_p, status_p|
          do_aes(
            key_binary_p,
            iv_binary_p,
            input_binary_p,
            output_binary_p,
            response_length_p,
            status_p
          )
        end

        @aes_decrypt = Proc.new do |_, key_binary_p, iv_binary_p, input_binary_p,
          output_binary_p, response_length_p, status_p|
          do_aes(
            key_binary_p,
            iv_binary_p,
            input_binary_p,
            output_binary_p,
            response_length_p,
            status_p,
            decrypt: true
          )
        end

        @random = Proc.new do |_, output_binary_p, num_bytes, status_p|
          write_binary_string_and_set_status(output_binary_p, status_p) do
            Hooks.random(num_bytes)
          end
        end

        @hmac_sha_512 = Proc.new do |_, key_binary_p, input_binary_p,
          output_binary_p, status_p|
          do_hmac_sha('SHA512', key_binary_p, input_binary_p, output_binary_p, status_p)
        end

        @hmac_sha_256 = Proc.new do |_, key_binary_p, input_binary_p,
          output_binary_p, status_p|
          do_hmac_sha('SHA256', key_binary_p, input_binary_p, output_binary_p, status_p)
        end

        @hmac_hash = Proc.new do |_, input_binary_p, output_binary_p, status_p|
          input = Binary.from_pointer(input_binary_p).to_s

          write_binary_string_and_set_status(output_binary_p, status_p) do
            Hooks.hash_sha256(input)
          end
        end

        Binding.setopt_crypto_hooks(
          self,
          @aes_encrypt,
          @aes_decrypt,
          @random,
          @hmac_sha_512,
          @hmac_sha_256,
          @hmac_hash,
        )

        @aes_ctr_encrypt = Proc.new do |_, key_binary_p, iv_binary_p, input_binary_p,
          output_binary_p, response_length_p, status_p|
          do_aes(
            key_binary_p,
            iv_binary_p,
            input_binary_p,
            output_binary_p,
            response_length_p,
            status_p,
            mode: :CTR,
          )
        end

        @aes_ctr_decrypt = Proc.new do |_, key_binary_p, iv_binary_p, input_binary_p,
          output_binary_p, response_length_p, status_p|
          do_aes(
            key_binary_p,
            iv_binary_p,
            input_binary_p,
            output_binary_p,
            response_length_p,
            status_p,
            decrypt: true,
            mode: :CTR,
          )
        end

        Binding.setopt_aes_256_ctr(
          self,
          @aes_ctr_encrypt,
          @aes_ctr_decrypt,
        )

        @rsaes_pkcs_signature_cb = Proc.new do |_, key_binary_p, input_binary_p,
          output_binary_p, status_p|
          do_rsaes_pkcs_signature(key_binary_p, input_binary_p, output_binary_p, status_p)
        end

        Binding.setopt_crypto_hook_sign_rsaes_pkcs1_v1_5(
          self,
          @rsaes_pkcs_signature_cb
        )
      end

      # Initialize the underlying mongocrypt_t object and raise an error if the operation fails
      def initialize_mongocrypt
        Binding.init(self)
        # There is currently no test for the error(?) code path
      end
    end
  end
end
