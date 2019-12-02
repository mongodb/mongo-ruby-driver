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

      attr_reader :mongocryptd_client, :mongocryptd_pid

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
        extra_options = options.delete(:extra_options)
        extra_options = DEFAULT_EXTRA_OPTIONS.merge(extra_options)

        monitoring_io_option = extra_options.delete(:mongocryptd_client_monitoring_io)

        if monitoring_io_option.nil?
          mongocryptd_client_monitoring_io = true
        else
          mongocryptd_client_monitoring_io = monitoring_io_option
        end

        opts_copy = options.dup
        opts_copy[:bypass_auto_encryption] = opts_copy[:bypass_auto_encryption] || false

        super(opts_copy.merge(extra_options))

        @mongocryptd_client = Client.new(
                                @encryption_options[:mongocryptd_uri],
                                monitoring_io: mongocryptd_client_monitoring_io,
                              )

        unless @encryption_options[:mongocryptd_bypass_spawn] || !mongocryptd_client_monitoring_io
          self.spawn_mongocryptd
        end

        # TODO: use all the other options for auto-encryption/auto-decryption
      end

      # Spawn a new mongocryptd process using the mongocryptd_spawn_path
      # and mongocryptd_spawn_args passed in through the extra auto
      # encrypt options. Stdout and Stderr of this new process are written
      # to /dev/null.
      #
      # @return [ Integer ] The process id of the spawned process
      def spawn_mongocryptd
        @mongocryptd_pid = Process.spawn(
                            @encryption_options[:mongocryptd_spawn_path],
                            *@encryption_options[:mongocryptd_spawn_args],
                            [:out, :err]=>'/dev/null'
                          )
      end

      # Abort the spawned mongocryptd process. Will not raise an error,
      # even if the process has already been terminated.
      #
      # @return [ true ] Always true
      def kill_mongocryptd
        return true if @mongocryptd_pid.nil?

        begin
          Process.kill('ABRT', @mongocryptd_pid)
          Process.wait(@mongocryptd_pid)
        rescue Errno::ESRCH
         log_warn("There was no mongocrypd process running at #{@mongocryptd_pid}.")
        end

        @mongocryptd_pid = nil

        true
      end

      # Close the resources created by the AutoEncrypter
      #
      # @return [ true ] Always true
      def teardown_encrypter
        @mongocryptd_client.close if @mongocryptd_client
        kill_mongocryptd

        true
      end
    end
  end
end
