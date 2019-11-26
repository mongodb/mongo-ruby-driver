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

    # TODO: documentation
    module AutoEncrypter
      include Encrypter

      def self.included(base)
        base.send :extend, Encrypter
      end

      # TODO: documentation
      def set_encryption_options(options = {})
        extra_options = options.delete(:extra_options) || {}
        extra_options = default_extra_options.merge(extra_options)

        super(
          options.tap do |options|
            options[:bypass_auto_encryption] = options[:bypass_auto_encryption] || false
            options[:key_vault_client] = options[:key_vault_client] || self
          end.merge(extra_options)
        )

        @mongocryptd_client = Client.new(@encryption_options[:mongocryptd_uri])
      end

      private

      def default_extra_options
        {
          mongocryptd_uri: 'mongodb://localhost:27020',
          mongocryptd_bypass_spawn: false,
          mongocryptd_spawn_path: '',
          mongocryptd_spawn_args: ['--idleShutdownTimeoutSecs=60'],
        }
      end
    end
  end
end
