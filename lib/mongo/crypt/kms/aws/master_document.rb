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
      module AWS
        # AWS KMS master key document object contains KMS master key parameters.
        #
        # @api private
        class MasterKeyDocument
          include KMS::Validations

          # @return [ String ] AWS region.
          attr_reader :region

          # @return [ String ] AWS KMS key.
          attr_reader :key

          # @return [ String | nil ] AWS KMS endpoint.
          attr_reader :endpoint

          FORMAT_HINT = "AWS key document  must be in the format: " +
                        "{ region: 'REGION', key: 'KEY' }"

          # Creates a master key document object form a parameters hash.
          #
          # @param [ Hash ] opts A hash that contains master key options for
          #   the AWS KMS provider.
          # @option opts [ String ] :region AWS region.
          # @option opts [ String ] :key AWS KMS key.
          # @option opts [ String | nil ] :endpoint AWS KMS endpoint, optional.
          #
          # @raise [ ArgumentError ] If required options are missing or incorrectly.
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

          # Convert master key document object to a BSON document in libmongocrypt format.
          #
          # @return [ BSON::Document ] AWS KMS master key document in libmongocrypt format.
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
