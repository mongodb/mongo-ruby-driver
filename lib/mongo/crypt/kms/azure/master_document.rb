# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2021 MongoDB Inc.
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
    module KMS
      module Azure
        # Azure KMS master key document object contains KMS master key parameters.
        #
        # @api private
        class MasterKeyDocument
          include KMS::Validations

          # @return [ String ] Azure key vault endpoint.
          attr_reader :key_vault_endpoint

          # @return [ String ] Azure KMS key name.
          attr_reader :key_name

          # @return [ String | nil ] Azure KMS key version.
          attr_reader :key_version

          FORMAT_HINT = "Azure key document  must be in the format: " +
                        "{ key_vault_endpoint: 'KEY_VAULT_ENDPOINT', key_name: 'KEY_NAME' }"

        # Creates a master key document object form a parameters hash.
        #
        # @param [ Hash ] opts A hash that contains master key options for
        #   the Azure KMS provider.
        # @option opts [ String ] :key_vault_endpoint Azure key vault endpoint.
        # @option opts [ String ] :key_name Azure KMS key name.
        # @option opts [ String | nil ] :key_version Azure KMS key version, optional.
        #
        # @raise [ ArgumentError ] If required options are missing or incorrectly.
          def initialize(opts)
            unless opts.is_a?(Hash)
              raise ArgumentError.new(
                'Key document options must contain a key named :master_key with a Hash value'
              )
            end
            @key_vault_endpoint = validate_param(:key_vault_endpoint, opts, FORMAT_HINT)
            @key_name = validate_param(:key_name, opts, FORMAT_HINT)
            @key_version = validate_param(:key_version, opts, FORMAT_HINT, required: false)
          end

          # Convert master key document object to a BSON document in libmongocrypt format.
          #
          # @return [ BSON::Document ] Azure KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({
              provider: 'azure',
              keyVaultEndpoint: key_vault_endpoint,
              keyName: key_name,
            }).tap do |bson|
              unless key_version.nil?
                bson.update({ keyVersion: key_version })
              end
            end
          end
        end
      end
    end
  end
end

