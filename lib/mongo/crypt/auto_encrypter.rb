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

    # A module that encapsulates auto-encryption functionality
    #
    # @api private
    module AutoEncrypter
      include Encrypter

      attr_reader :mongocryptd_client

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
      # @option [ Mongo::Client ] :key_vault_client A client connected
      #   to the MongoDB instance containing the encryption key vault.
      # @option [ String ] :key_vault_namespace The namespace of the key
      #   vault in the format database.collection.
      # @option [ Hash | nil ] :schema_map The JSONSchema of the collection(s)
      #   with encrypted fields.
      # @option [ Boolean | nil ] :bypass_auto_encryption When true, disables
      #   auto-encryption. Default is false.
      # @option [ Hash | nil ] :extra_options Options related to spawning
      #   mongocryptd. These are set to default values if no option is passed in.
      #
      # @raise [ ArgumentError ] If required options are missing or incorrectly
      #   formatted.
      def setup_encrypter(options = {})
        opts = set_default_options(options.dup)

        unless opts[:key_vault_client]
          # If no key vault client is passed in, create one by copying the
          # Mongo::Client used for encryption. Update options so that key vault
          # client does not perform auto-encryption/decryption, and keep a reference
          # to it so it is destroyed later.
          @key_vault_client = self.with({ auto_encryption_options: nil })

          opts[:key_vault_client] = @key_vault_client
        end

        mongocryptd_client_monitoring_io = opts.delete(:mongocryptd_client_monitoring_io)
        mongocryptd_client_monitoring_io = true if mongocryptd_client_monitoring_io.nil?

        super(opts)

        @mongocryptd_client = Client.new(
                                @encryption_options[:mongocryptd_uri],
                                monitoring_io: mongocryptd_client_monitoring_io,
                              )

        # TODO: use all the other options for auto-encryption/auto-decryption
      end

      # Spawn a new mongocryptd process using the mongocryptd_spawn_path
      # and mongocryptd_spawn_args passed in through the extra auto
      # encrypt options. Stdout and Stderr of this new process are written
      # to /dev/null.
      #
      # @note To capture the mongocryptd logs, add "--logpath=/path/to/logs"
      #   to auto_encryption_options -> extra_options -> mongocrpytd_spawn_args
      #
      # @return [ Integer ] The process id of the spawned process
      #
      # @raise [ ArgumentError ] Raises an exception if no encryption options
      #   have been provided
      def spawn_mongocryptd
        unless @encryption_options
          raise ArgumentError.new('Cannot spawn mongocryptd process without setting auto encryption options on the client.')
        end

        mongocryptd_spawn_args = @encryption_options[:mongocryptd_spawn_args]

        Process.spawn(
          @encryption_options[:mongocryptd_spawn_path],
          *mongocryptd_spawn_args,
          [:out, :err]=>'/dev/null'
        )
      end

      # Close the resources created by the AutoEncrypter
      #
      # @return [ true ] Always true
      def teardown_encrypter
        @mongocryptd_client.close if @mongocryptd_client
        @key_vault_client.close if @key_vault_client

        @mongocryptd_client = nil
        @key_vault_client = nil
        @encryption_options = nil

        true
      end

      private

      # Sets the following default options:
      # - default values for all extra_options
      # - adds --idleShtudownTimeoutSecs=60 to extra_options[:mongocryptd_spawn_args]
      #   if not already present
      # - sets bypass_auto_encryption to false
      def set_default_options(options)
        opts = options.dup

        extra_options = opts.delete(:extra_options)
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

        opts.merge(extra_options)
      end
    end
  end
end
