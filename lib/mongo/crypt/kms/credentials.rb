# frozen_string_literal: true

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
      # KMS Credentials object contains credentials for using KMS providers.
      #
      # @api private
      class Credentials
        # Creates a KMS credentials object from a parameters hash.
        #
        # @param [ Hash ] kms_providers A hash that contains credentials for
        #   KMS providers. Keys may be provider types (:aws, :local, etc.) or
        #   named provider identifiers ("aws:name1", "local:name2", etc.).
        #   Values are hashes of credentials for the corresponding provider type.
        #
        # @note There may be more than one KMS provider specified.
        #
        # @raise [ ArgumentError ] If required options are missing or incorrectly
        #   formatted.
        def initialize(kms_providers)
          raise ArgumentError.new('KMS providers options must not be nil') if kms_providers.nil?

          @credentials_map = {}

          kms_providers.each do |identifier, opts|
            identifier_str = identifier.to_s
            provider_type = identifier_str.split(':').first

            creds = case provider_type
                    when 'aws' then AWS::Credentials.new(opts)
                    when 'azure' then Azure::Credentials.new(opts)
                    when 'gcp' then GCP::Credentials.new(opts)
                    when 'kmip' then KMIP::Credentials.new(opts)
                    when 'local' then Local::Credentials.new(opts)
                    else
                      raise ArgumentError.new(
                        'KMS providers options must have one of the following keys: ' \
                        ':aws, :azure, :gcp, :kmip, :local'
                      )
                    end

            @credentials_map[identifier_str] = creds
          end

          return unless @credentials_map.empty?

          raise ArgumentError.new(
            'KMS providers options must have one of the following keys: ' \
            ':aws, :azure, :gcp, :kmip, :local'
          )
        end

        # @return [ Credentials::AWS | nil ] AWS KMS credentials (unnamed provider only).
        def aws
          @credentials_map['aws']
        end

        # @return [ Credentials::Azure | nil ] Azure KMS credentials (unnamed provider only).
        def azure
          @credentials_map['azure']
        end

        # @return [ Credentials::GCP | nil ] GCP KMS credentials (unnamed provider only).
        def gcp
          @credentials_map['gcp']
        end

        # @return [ Credentials::KMIP | nil ] KMIP KMS credentials (unnamed provider only).
        def kmip
          @credentials_map['kmip']
        end

        # @return [ Credentials::Local | nil ] Local KMS credentials (unnamed provider only).
        def local
          @credentials_map['local']
        end

        # Convert credentials object to a BSON document in libmongocrypt format.
        #
        # @return [ BSON::Document ] Credentials as BSON document.
        def to_document
          BSON::Document.new.tap do |bson|
            @credentials_map.each do |identifier, creds|
              bson[identifier] = creds.to_document
            end
          end
        end
      end
    end
  end
end
