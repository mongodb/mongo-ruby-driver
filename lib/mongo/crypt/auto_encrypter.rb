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

      # A Hash of default values for the :extra_options option
      DEFAULT_EXTRA_OPTIONS = {
        mongocryptd_uri: 'mongodb://localhost:27020',
        mongocryptd_bypass_spawn: false,
        mongocryptd_spawn_path: '',
        mongocryptd_spawn_args: ['--idleShutdownTimeoutSecs=60'],
      }.freeze

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
        extra_options = options.delete(:extra_options) || {}
        extra_options = DEFAULT_EXTRA_OPTIONS.merge(extra_options)

        super(
          options.tap do |options|
            options[:bypass_auto_encryption] = options[:bypass_auto_encryption] || false
          end.merge(extra_options)
        )

        @mongocryptd_client = Client.new(@encryption_options[:mongocryptd_uri])

        # TODO: use all the other options for auto-encryption/auto-decryption
      end

      def shutdown_encrypter
        @mongocryptd_client.close if @mongocryptd_client
      end
    end
  end
end
