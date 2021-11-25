# frozen_string_literal: true
# encoding: utf-8

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
        class Credentials
          include KMS::Validations

          # @return [ String ] Azure tenant id.
          attr_reader :tenant_id

          # @return [ String ] Azure client id.
          attr_reader :client_id

          # @return [ String ] Azure client secret.
          attr_reader :client_secret

          # @return [ String | nil ] Azure identity platform endpoint.
          attr_reader :identity_platform_endpoint

          FORMAT_HINT = "Azure KMS provider options must be in the format: " +
              "{ tenant_id: 'TENANT-ID', client_id: 'TENANT_ID', client_secret: 'CLIENT_SECRET' }"

          def initialize(opts)
            @tenant_id = validate_param(:tenant_id, opts, FORMAT_HINT)
            @client_id = validate_param(:client_id, opts, FORMAT_HINT)
            @client_secret = validate_param(:client_secret, opts, FORMAT_HINT)
            @identity_platform_endpoint = validate_param(
              :identity_platform_endpoint, opts, FORMAT_HINT, required: false
            )
          end

          # @return [ BSON::Document ] Azure KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({
              tenantId: @tenant_id,
              clientId: @client_id,
              clientSecret: @client_secret,
            }).tap do |bson|
              unless identity_platform_endpoint.nil?
                bson.update({ identityPlatformEndpoint: identity_platform_endpoint })
              end
            end
          end
        end

        class KeyDocument
          include KMS::Validations

          attr_reader :key_vault_endpoint
          attr_reader :key_name
          attr_reader :key_version
          FORMAT_HINT = "Azure key document  must be in the format: " +
                        "{ key_vault_endpoint: 'KEY_VAULT_ENDPOINT', key_name: 'KEY_NAME' }"

          def initialize(opts)
            if opts.is_a?(Hash)
              raise ArgumentError.new(
                'Key document options must contain a key named :master_key with a Hash value'
              )
            end
            @key_vault_endpoint = validate_param(:key_vault_endpoint, opts, FORMAT_HINT)
            @key_name = validate_param(:key_name, opts, FORMAT_HINT)
            @key_version = validate_param(:key_version, opts, FORMAT_HINT, required: false)
          end

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
