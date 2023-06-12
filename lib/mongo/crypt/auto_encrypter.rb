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

    # An AutoEcnrypter is an object that encapsulates the behavior of
    # automatic encryption. It controls all resources associated with
    # auto-encryption, including the libmongocrypt handle, key vault client
    # object, mongocryptd client object, and encryption I/O.
    #
    # The AutoEncrypter is kept as an instance on a Mongo::Client. Client
    # objects with the same auto_encryption_options Hash may share
    # AutoEncrypters.
    #
    # @api private
    class AutoEncrypter

      attr_reader :mongocryptd_client
      attr_reader :key_vault_client
      attr_reader :metadata_client
      attr_reader :options

      # A Hash of default values for the :extra_options option
      DEFAULT_EXTRA_OPTIONS = Options::Redacted.new({
        mongocryptd_uri: 'mongodb://localhost:27020',
        mongocryptd_bypass_spawn: false,
        mongocryptd_spawn_path: 'mongocryptd',
        mongocryptd_spawn_args: ['--idleShutdownTimeoutSecs=60'],
      })

      # Set up encryption-related options and instance variables
      # on the class that includes this module. Calls the same method
      # on the Mongo::Crypt::Encrypter module.
      #
      # @param [ Hash ] options
      #
      # @option options [ Mongo::Client ] :client A client connected to the
      #   encrypted collection.
      # @option options [ Mongo::Client | nil ] :key_vault_client A client connected
      #   to the MongoDB instance containing the encryption key vault; optional.
      #   If not provided, will default to :client option.
      # @option options [ String ] :key_vault_namespace The namespace of the key
      #   vault in the format database.collection.
      # @option options [ Hash | nil ] :schema_map The JSONSchema of the collection(s)
      #   with encrypted fields. This option is mutually exclusive with :schema_map_path.
      # @option options [ String | nil ] :schema_map_path A path to a file contains the JSON schema
      #   of the collection that stores auto encrypted documents. This option is
      #   mutually exclusive with :schema_map.
      # @option options [ Boolean | nil ] :bypass_auto_encryption When true, disables
      #   auto-encryption. Default is false.
      # @option options [ Hash | nil ] :extra_options Options related to spawning
      #   mongocryptd. These are set to default values if no option is passed in.
      # @option options [ Hash ] :kms_providers A hash of key management service
      #   configuration information.
      #   @see Mongo::Crypt::KMS::Credentials for list of options for every
      #   supported provider.
      #   @note There may be more than one KMS provider specified.
      # @option options [ Hash ] :kms_tls_options TLS options to connect to KMS
      #   providers. Keys of the hash should be KSM provider names; values
      #   should be hashes of TLS connection options. The options are equivalent
      #   to TLS connection options of Mongo::Client.
      #   @see Mongo::Client#initialize for list of TLS options.
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
      #   crypt shared library is required. If 'true', an error will be raised
      #   if a crypt_shared library cannot be loaded by libmongocrypt.
      #
      # @raise [ ArgumentError ] If required options are missing or incorrectly
      #   formatted.
      def initialize(options)
        Crypt.validate_ffi!
        # Note that this call may eventually, via other method invocations,
        # create additional clients which have to be cleaned up.
        @options = set_default_options(options).freeze

        @crypt_handle = Crypt::Handle.new(
          Crypt::KMS::Credentials.new(@options[:kms_providers]),
          Crypt::KMS::Validations.validate_tls_options(@options[:kms_tls_options]),
          schema_map: @options[:schema_map],
          schema_map_path: @options[:schema_map_path],
          encrypted_fields_map: @options[:encrypted_fields_map],
          bypass_query_analysis: @options[:bypass_query_analysis],
          crypt_shared_lib_path: @options[:extra_options][:crypt_shared_lib_path],
          crypt_shared_lib_required: @options[:extra_options][:crypt_shared_lib_required],
        )

        @mongocryptd_options = @options[:extra_options].slice(
          :mongocryptd_uri,
          :mongocryptd_bypass_spawn,
          :mongocryptd_spawn_path,
          :mongocryptd_spawn_args
        )
        @mongocryptd_options[:mongocryptd_bypass_spawn] = @options[:bypass_auto_encryption] ||
          @options[:extra_options][:mongocryptd_bypass_spawn] ||
          @crypt_handle.crypt_shared_lib_available? ||
          @options[:extra_options][:crypt_shared_lib_required]

        unless @options[:extra_options][:crypt_shared_lib_required] || @crypt_handle.crypt_shared_lib_available? || @options[:bypass_query_analysis]
          # Set server selection timeout to 1 to prevent the client waiting for a
          # long timeout before spawning mongocryptd
          @mongocryptd_client = Client.new(
            @options[:extra_options][:mongocryptd_uri],
            monitoring_io: @options[:client].options[:monitoring_io],
            populator_io: @options[:client].options[:populator_io],
            server_selection_timeout: 10,
            database: @options[:client].options[:database]
          )
        end

        begin
          @encryption_io = EncryptionIO.new(
            client: @options[:client],
            mongocryptd_client: @mongocryptd_client,
            key_vault_namespace: @options[:key_vault_namespace],
            key_vault_client: @key_vault_client,
            metadata_client: @metadata_client,
            mongocryptd_options: @mongocryptd_options
          )
        rescue
          begin
            @mongocryptd_client&.close
          rescue => e
            log_warn("Error closing mongocryptd client in auto encrypter's constructor: #{e.class}: #{e}")
            # Drop this exception so that the original exception is raised
          end
          raise
        end
      rescue
        if @key_vault_client && @key_vault_client != options[:client] &&
          @key_vault_client.cluster != options[:client].cluster
        then
          begin
            @key_vault_client.close
          rescue => e
            log_warn("Error closing key vault client in auto encrypter's constructor: #{e.class}: #{e}")
            # Drop this exception so that the original exception is raised
          end
        end

        if @metadata_client && @metadata_client != options[:client] &&
          @metadata_client.cluster != options[:client].cluster
        then
          begin
            @metadata_client.close
          rescue => e
            log_warn("Error closing metadata client in auto encrypter's constructor: #{e.class}: #{e}")
            # Drop this exception so that the original exception is raised
          end
        end

        raise
      end

      # Whether this encrypter should perform encryption (returns false if
      # the :bypass_auto_encryption option is set to true).
      #
      # @return [ Boolean ] Whether to perform encryption.
      def encrypt?
        !@options[:bypass_auto_encryption]
      end

      # Encrypt a database command.
      #
      # @param [ String ] database_name The name of the database on which the
      #   command is being run.
      # @param [ Hash ] command The command to be encrypted.
      #
      # @return [ BSON::Document ] The encrypted command.
      def encrypt(database_name, command)
        AutoEncryptionContext.new(
          @crypt_handle,
          @encryption_io,
          database_name,
          command
        ).run_state_machine
      end

      # Decrypt a database command.
      #
      # @param [ Hash ] command The command with encrypted fields.
      #
      # @return [ BSON::Document ] The decrypted command.
      def decrypt(command)
        AutoDecryptionContext.new(
          @crypt_handle,
          @encryption_io,
          command
        ).run_state_machine
      end

      # Close the resources created by the AutoEncrypter.
      #
      # @return [ true ] Always true.
      def close
        @mongocryptd_client.close if @mongocryptd_client

        if @key_vault_client && @key_vault_client != options[:client] &&
          @key_vault_client.cluster != options[:client].cluster
        then
          @key_vault_client.close
        end

        if @metadata_client && @metadata_client != options[:client] &&
          @metadata_client.cluster != options[:client].cluster
        then
          @metadata_client.close
        end

        true
      end

      private

      # Returns a new set of options with the following changes:
      # - sets default values for all extra_options
      # - adds --idleShtudownTimeoutSecs=60 to extra_options[:mongocryptd_spawn_args]
      #   if not already present
      # - sets bypass_auto_encryption to false
      # - sets default key vault client
      def set_default_options(options)
        opts = options.dup

        extra_options = opts.delete(:extra_options) || Options::Redacted.new
        extra_options = DEFAULT_EXTRA_OPTIONS.merge(extra_options)

        has_timeout_string_arg = extra_options[:mongocryptd_spawn_args].any? do |elem|
          elem.is_a?(String) && elem.match(/\A--idleShutdownTimeoutSecs=\d+\z/)
        end

        timeout_int_arg_idx = extra_options[:mongocryptd_spawn_args].index('--idleShutdownTimeoutSecs')
        has_timeout_int_arg = timeout_int_arg_idx && extra_options[:mongocryptd_spawn_args][timeout_int_arg_idx + 1].is_a?(Integer)

        unless has_timeout_string_arg || has_timeout_int_arg
          extra_options[:mongocryptd_spawn_args] << '--idleShutdownTimeoutSecs=60'
        end

        opts[:bypass_auto_encryption] ||= false
        set_or_create_clients(opts)
        opts[:key_vault_client] = @key_vault_client

        Options::Redacted.new(opts).merge(extra_options: extra_options)
      end

      # Create additional clients for auto encryption, if necessary
      #
      # @param [ Hash ] options Auto encryption options.
      def set_or_create_clients(options)
        client = options[:client]
        @key_vault_client = if options[:key_vault_client]
          options[:key_vault_client]
        elsif client.options[:max_pool_size] == 0
          client
        else
          internal_client(client)
        end

        @metadata_client = if options[:bypass_auto_encryption]
          nil
        elsif client.options[:max_pool_size] == 0
          client
        else
          internal_client(client)
        end
      end

      # Creates or return already created internal client to be used for
      # auto encryption.
      #
      # @param [ Mongo::Client ] client  A client connected to the
      #   encrypted collection.
      #
      # @return [ Mongo::Client ] Client to be used as internal client for
      # auto encryption.
      def internal_client(client)
        @internal_client ||= client.with(
          auto_encryption_options: nil,
          min_pool_size: 0,
          monitoring: client.send(:monitoring),
        )
      end
    end
  end
end
