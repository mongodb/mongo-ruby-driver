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

      # KMS master key document object contains KMS master key parameters
      # that are used for creation of data keys.
      #
      # @api private
      class MasterKeyDocument

        # Known KMS provider names.
        KMS_PROVIDERS = %w(aws azure gcp kmip local).freeze

        # Creates a master key document object form a parameters hash.
        #
        # @param [ String ] kms_provider. KMS provider name.
        # @param [ Hash ] options A hash that contains master key options for
        #   the KMS provider.
        #   Required parameters for KMS providers are described in corresponding
        #   classes inside Mongo::Crypt::KMS module.
        #
        # @raise [ ArgumentError ] If required options are missing or incorrectly.
        def initialize(kms_provider, options)
          if options.nil?
            raise ArgumentError.new('Key document options must not be nil')
          end
          master_key = options.fetch(:master_key, {})
          @key_document = case kms_provider.to_s
            when 'aws' then KMS::AWS::MasterKeyDocument.new(master_key)
            when 'azure' then KMS::Azure::MasterKeyDocument.new(master_key)
            when 'gcp' then KMS::GCP::MasterKeyDocument.new(master_key)
            when 'kmip' then KMS::KMIP::MasterKeyDocument.new(master_key)
            when 'local' then KMS::Local::MasterKeyDocument.new(master_key)
            else
              raise ArgumentError.new("KMS provider must be one of #{KMS_PROVIDERS}")
          end
        end

        # Convert master key document object to a BSON document in libmongocrypt format.
        #
        # @return [ BSON::Document ] Master key document as BSON document.
        def to_document
          @key_document.to_document
        end
      end
    end
  end
end
