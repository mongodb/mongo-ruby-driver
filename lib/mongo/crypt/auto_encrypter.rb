# frozen_string_literal: true
# encoding: utf-8

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
      #   with encrypted fields.
      # @option options [ Boolean | nil ] :bypass_auto_encryption When true, disables
      #   auto-encryption. Default is false.
      # @option options [ Hash | nil ] :extra_options Options related to spawning
      #   mongocryptd. These are set to default values if no option is passed in.
      #
      # @raise [ ArgumentError ] If required options are missing or incorrectly
      #   formatted.
      def initialize(options)
        @options = set_default_options(options).freeze

        @crypt_handle = Crypt::Handle.new(
          @options[:kms_providers],
          schema_map: @options[:schema_map]
        )

        @key_vault_client = @options[:key_vault_client]

        # Set server selection timeout to 1 to prevent the client waiting for a
        # long timeout before spawning mongocryptd
        @mongocryptd_client = Client.new(
          @options[:extra_options][:mongocryptd_uri],
          monitoring_io: @options[:client].options[:monitoring_io],
          server_selection_timeout: 10,
        )

        begin
          @encryption_io = EncryptionIO.new(
            client: @options[:client],
            mongocryptd_client: @mongocryptd_client,
            key_vault_namespace: @options[:key_vault_namespace],
            key_vault_client: @key_vault_client,
            mongocryptd_options: @options[:extra_options]
          )
        rescue
          begin
            @mongocryptd_client.close
          rescue => e
            log_warn("Eror closing mongocryptd client in auto encrypter's constructor: #{e.class}: #{e}")
            # Drop this exception so that the original exception is raised
          end
          raise
        end
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
        opts[:key_vault_client] ||= opts[:client]

        Options::Redacted.new(opts).merge(extra_options: extra_options)
      end
    end
  end
end
