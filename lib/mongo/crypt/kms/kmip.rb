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
      module KMIP
        # KMIP KMS Credentials object contains credentials for a
        # remote KMIP KMS provider.
        #
        # @api private
        class Credentials
          extend Forwardable
          include KMS::Validations

          # @return [ String ] KMIP KMS endpoint with optional port.
          attr_reader :endpoint

          # @api private
          def_delegator :@opts, :empty?

          FORMAT_HINT = "KMIP KMS provider options must be in the format: " +
                        "{ endpoint: 'ENDPOINT' }"

          # Creates a KMIP KMS credentials object form a parameters hash.
          #
          # @param [ Hash ] opts A hash that contains credentials for
          #   KMIP KMS provider.
          # @option opts [ String ] :endpoint KMIP endpoint.
          #
          # @raise [ ArgumentError ] If required options are missing or incorrectly
          #   formatted.
          def initialize(opts)
            @opts = opts
            unless empty?
              @endpoint = validate_param(:endpoint, opts, FORMAT_HINT)
            end
          end

          # Convert credentials object to a BSON document in libmongocrypt format.
          #
          # @return [ BSON::Document ] Local KMS credentials in libmongocrypt format.
          def to_document
            return BSON::Document.new({}) if empty?
            BSON::Document.new({
              endpoint: endpoint,
            })
          end
        end

        # KMIP KMS master key document object contains KMS master key parameters.
        #
        # @api private
        class MasterKeyDocument
          include KMS::Validations

          # @return [ String | nil ] The KMIP Unique Identifier to a 96 byte
          #   KMIP Secret Data managed object.
          attr_reader :key_id

          # @return [ String | nil ] KMIP KMS endpoint with optional port.
          attr_reader :endpoint

          FORMAT_HINT = "KMIP KMS key document must be in the format: " +
                        "{ key_id: 'KEY-ID', endpoint: 'ENDPOINT' }"

          # Creates a master key document object form a parameters hash.
          #
          # @param [ Hash ] opts A hash that contains master key options for
          #   KMIP KMS provider
          # @option opts [ String | nil ] :key_id KMIP Unique Identifier to
          #   a 96 byte KMIP Secret Data managed object, optional. If key_id
          #   is omitted, the driver creates a random 96 byte identifier.
          # @option opts [ String | nil ] :endpoint KMIP endpoint, optional.
          #
          # @raise [ ArgumentError ] If required options are missing or incorrectly
          #   formatted.
          def initialize(opts = {})
            @key_id = validate_param(
              :key_id, opts, FORMAT_HINT, required: false
            )
            @endpoint = validate_param(
              :endpoint, opts, FORMAT_HINT, required: false
            )
          end

          # Convert master key document object to a BSON document in libmongocrypt format.
          #
          # @return [ BSON::Document ] KMIP KMS credentials in libmongocrypt format.
          def to_document
            BSON::Document.new({
              provider: 'kmip',
            }).tap do |bson|
              bson.update({ endpoint: endpoint }) unless endpoint.nil?
              bson.update({ keyId: key_id }) unless key_id.nil?
            end
          end
        end
      end
    end
  end
end
