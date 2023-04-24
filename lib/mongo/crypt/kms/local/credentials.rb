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
      module Local
        # Local KMS Credentials object contains credentials for using local KMS provider.
        #
        # @api private
        class Credentials
          extend Forwardable
          include KMS::Validations

          # @return [ String ] Master key.
          attr_reader :key

          # @api private
          def_delegator :@opts, :empty?

          FORMAT_HINT = "Local KMS provider options must be in the format: " +
                        "{ key: 'MASTER-KEY' }"

          # Creates a local KMS credentials object form a parameters hash.
          #
          # @param [ Hash ] opts A hash that contains credentials for
          #   local KMS provider
          # @option opts [ String ] :key Master key.
          #
          # @raise [ ArgumentError ] If required options are missing or incorrectly
          #   formatted.
          def initialize(opts)
            @opts = opts
            unless empty?
              @key = validate_param(:key, opts, FORMAT_HINT)
            end
          end

          # @return [ BSON::Document ] Local KMS credentials in libmongocrypt format.
          def to_document
            return BSON::Document.new({}) if empty?
            BSON::Document.new({
              key: BSON::Binary.new(@key, :generic),
            })
          end
        end
      end
    end
  end
end

