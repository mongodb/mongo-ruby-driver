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
      class Credentials
        attr_reader :aws

        attr_reader :azure

        attr_reader :local

        def initialize(opts = {})
          if opts.key?(:aws)
            @aws = AWS.from_hash(opts[:aws])
          end
          if opts.key?(:azure)
            @azure = Azure.from_hash(opts[:azure])
          end
          if opts.key?(:local)
            @local = Local.from_hash(opts[:local])
          end
        end

        def to_bson
          BSON::Document.new({}).tap do |bson|
            bson[:aws] = @aws.to_bson if @aws
            bson[:azure] = @azure.to_bson if @azure
            bson[:local] = @local.to_bson if @local
          end
        end

        module Base
          # Validate if a KMS parameter is valid.
          #
          # @param [ Symbol ] key The parameter name.
          # @param [ Hash ] opts Hash should contain the parameter under the key.
          # @param [ Boolean ] required Whether the parameter is required or not.
          #   Non-required parameters can be nil.
          #
          # @return [ String | nil ] String parameter value or nil if a
          #   non-required parameter is missing.
          #
          # @raise [ ArgumentError ] If required options are missing or incorrectly
          #   formatted.
          def validate_param(key, opts, required: true)
            value = opts.fetch(key)
            if value.nil?
              raise ArgumentError.new(
                "The #{key} option must be a String with at least one character; " \
                "currently have nil"
              )
            end
            unless value.is_a?(String)
              raise ArgumentError.new(
                "The #{key} option must be a String with at least one character; " \
                "currently have #{value}"
              )
            end
            if value.empty?
              raise ArgumentError.new(
                "The #{key} option must be a String with at least one character; " \
                "it is currently an empty string"
              )
            end
            value
          rescue KeyError
            if required
              raise ArgumentError.new(
                "The specified KMS provider options are invalid: #{opts}. " +
                format_hint
              )
            else
              nil
            end
          end
        end

        class AWS
          extend Base

          # @return [ String ] AWS access key
          attr_reader :access_key_id

          # @return [ String ] AWS secret access key
          attr_reader :secret_access_key

          # @return [ String | nil ] AWS session token
          attr_reader :session_token

          def self.format_hint
            "AWS KMS provider options must be in the format: " +
              "{ access_key_id: 'YOUR-ACCESS-KEY-ID', secret_access_key: 'SECRET-ACCESS-KEY' }"
          end

          def self.from_hash(opts = {})
            new(
              access_key_id: validate_param(:access_key_id, opts),
              secret_access_key: validate_param(:secret_access_key, opts),
              session_token: validate_param(:session_token, opts, required: false),
            )
          end

          def initialize(access_key_id:, secret_access_key:, session_token: nil)
            @access_key_id = access_key_id
            @secret_access_key = secret_access_key
            @session_token = session_token
          end

          # @return [ BSON::Document ] AWS KMS credentials in libmongocrypt format.
          def to_bson
            BSON::Document.new({
              accessKeyId: @access_key_id,
              secretAccessKey: @secret_access_key,
            }).tap do |bson|
              unless session_token.nil?
                bson.update({ sessionToken: session_token })
              end
            end
          end
        end

        class Azure
          extend Base

          # @return [ String ] Azure tenant id.
          attr_reader :tenant_id

          # @return [ String ] Azure client id.
          attr_reader :client_id

          # @return [ String ] Azure client secret.
          attr_reader :client_secret

          # @return [ String | nil ] Azure identity platform endpoint.
          attr_reader :identity_platform_endpoint

          def self.format_hint
            "Azure KMS provider options must be in the format: " +
              "{ tenant_id: 'TENANT-ID', client_id: 'TENANT_ID', client_secret: 'CLIENT_SECRET' }"
          end

          def self.from_hash(opts = {})
            new(
              tenant_id: validate_param(:tenant_id, opts),
              client_id: validate_param(:client_id, opts),
              client_secret: validate_param(:client_secret, opts),
              identity_platform_endpoint: validate_param(:identity_platform_endpoint, opts, required: false),
            )
          end

          def initialize(tenant_id:, client_id:, client_secret:, identity_platform_endpoint: nil)
            @tenant_id = tenant_id
            @client_id = client_id
            @client_secret = client_secret
            @identity_platform_endpoint = identity_platform_endpoint
          end

          # @return [ BSON::Document ] Azure KMS credentials in libmongocrypt format.
          def to_bson
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

        class Local
          extend Base

          attr_reader :key

          def self.format_hint
            "Local KMS provider options must be in the format: " +
              "{ key: 'MASTER-KEY' }"
          end

          def self.from_hash(opts = {})
            new(
              key: validate_param(:key, opts),
            )
          end

          def initialize(key:)
            @key = key
          end

          # @return [ BSON::Document ] Azure KMS credentials in libmongocrypt format.
          def to_bson
            BSON::Document.new({
              key: BSON::Binary.new(@key, :generic),
            })
          end
        end
      end
    end
  end
end
