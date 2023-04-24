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
      module GCP
        # GCP KMS master key document object contains KMS master key parameters.
        #
        # @api private
        class MasterKeyDocument
          include KMS::Validations

          # @return [ String ] GCP project id.
          attr_reader :project_id

          # @return [ String ] GCP location.
          attr_reader :location

          # @return [ String ] GCP KMS key ring.
          attr_reader :key_ring

          # @return [ String ] GCP KMS key name.
          attr_reader :key_name

          # @return [ String | nil ] GCP KMS key version.
          attr_reader :key_version

          # @return [ String | nil ] GCP KMS endpoint.
          attr_reader :endpoint

          FORMAT_HINT = "GCP key document  must be in the format: " +
            "{ project_id: 'PROJECT_ID', location: 'LOCATION', " +
            "key_ring: 'KEY-RING', key_name: 'KEY-NAME' }"

          # Creates a master key document object form a parameters hash.
          #
          # @param [ Hash ] opts A hash that contains master key options for
          #   the GCP KMS provider.
          # @option opts [ String ] :project_id GCP  project id.
          # @option opts [ String ] :location GCP location.
          # @option opts [ String ] :key_ring GCP KMS key ring.
          # @option opts [ String ] :key_name GCP KMS key name.
          # @option opts [ String | nil ] :key_version GCP KMS key version, optional.
          # @option opts [ String | nil ] :endpoint GCP KMS key endpoint, optional.
          #
          # @raise [ ArgumentError ] If required options are missing or incorrectly.
          def initialize(opts)
            if opts.empty?
              @empty = true
              return
            end
            @project_id = validate_param(:project_id, opts, FORMAT_HINT)
            @location = validate_param(:location, opts, FORMAT_HINT)
            @key_ring = validate_param(:key_ring, opts, FORMAT_HINT)
            @key_name = validate_param(:key_name, opts, FORMAT_HINT)
            @key_version = validate_param(:key_version, opts, FORMAT_HINT, required: false)
            @endpoint = validate_param(:endpoint, opts, FORMAT_HINT, required: false)
          end

          # Convert master key document object to a BSON document in libmongocrypt format.
          #
          # @return [ BSON::Document ] GCP KMS credentials in libmongocrypt format.
          def to_document
            return BSON::Document.new({}) if @empty
            BSON::Document.new({
              provider: 'gcp',
              projectId: project_id,
              location: location,
              keyRing: key_ring,
              keyName: key_name
            }).tap do |bson|
              unless key_version.nil?
                bson.update({ keyVersion: key_version })
              end
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
