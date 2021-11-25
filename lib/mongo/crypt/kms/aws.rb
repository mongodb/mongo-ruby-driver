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
      module AWS
        class Credentials
          include KMS::Validations

          # @return [ String ] AWS access key
          attr_reader :access_key_id

          # @return [ String ] AWS secret access key
          attr_reader :secret_access_key

          # @return [ String | nil ] AWS session token
          attr_reader :session_token

          FORMAT_HINT = "AWS KMS provider options must be in the format: " +
                        "{ access_key_id: 'YOUR-ACCESS-KEY-ID', secret_access_key: 'SECRET-ACCESS-KEY' }"

          def initialize(opts)
            @access_key_id = validate_param(:access_key_id, opts, FORMAT_HINT)
            @secret_access_key = validate_param(:secret_access_key, opts, FORMAT_HINT)
            @session_token = validate_param(:session_token, opts, FORMAT_HINT, required: false)
          end

          # @return [ BSON::Document ] AWS KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({
              accessKeyId: access_key_id,
              secretAccessKey: secret_access_key,
            }).tap do |bson|
              unless session_token.nil?
                bson.update({ sessionToken: session_token })
              end
            end
          end
        end

        class KeyDocument
          include KMS::Validations

          attr_reader :region
          attr_reader :key
          attr_reader :endpoint
          FORMAT_HINT = "AWS key document  must be in the format: " +
                        "{ region: 'REGION', key: 'KEY' }"

          def initialize(opts)
            unless opts.is_a?(Hash)
              raise ArgumentError.new(
                'Key document options must contain a key named :master_key with a Hash value'
              )
            end
            @region = validate_param(:region, opts, FORMAT_HINT)
            @key = validate_param(:key, opts, FORMAT_HINT)
            @endpoint = validate_param(:endpoint, opts, FORMAT_HINT, required: false)
          end

          # @return [ BSON::Document ] AWS KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({
              provider: 'aws',
              region: region,
              key: key,
            }).tap do |bson|
              unless endpoint.nil?
                bson.update({ endpoint: endpoint })
              end
            end
          end
        end
      end
    end
  end
end
