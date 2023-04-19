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

      # KMS Credentials object contains credentials for using KMS providers.
      #
      # @api private
      class Credentials

        # @return [ Credentials::AWS | nil ] AWS KMS credentials.
        attr_reader :aws

        # @return [ Credentials::Azure | nil ] Azure KMS credentials.
        attr_reader :azure

        # @return [ Credentials::GCP | nil ] GCP KMS credentials.
        attr_reader :gcp

        # @return [ Credentials::KMIP | nil ] KMIP KMS credentials.
        attr_reader :kmip

        # @return [ Credentials::Local | nil ] Local KMS credentials.
        attr_reader :local

        # Creates a KMS credentials object form a parameters hash.
        #
        # @param [ Hash ] kms_providers A hash that contains credential for
        #   KMS providers. The hash should have KMS provider names as keys,
        #   and required parameters for every provider as values.
        #   Required parameters for KMS providers are described in corresponding
        #   classes inside Mongo::Crypt::KMS module.
        #
        # @note There may be more than one KMS provider specified.
        #
        # @raise [ ArgumentError ] If required options are missing or incorrectly
        #   formatted.
        def initialize(kms_providers)
          if kms_providers.nil?
            raise ArgumentError.new("KMS providers options must not be nil")
          end
          if kms_providers.key?(:aws)
            @aws = AWS::Credentials.new(kms_providers[:aws])
          end
          if kms_providers.key?(:azure)
            @azure = Azure::Credentials.new(kms_providers[:azure])
          end
          if kms_providers.key?(:gcp)
            @gcp = GCP::Credentials.new(kms_providers[:gcp])
          end
          if kms_providers.key?(:kmip)
            @kmip = KMIP::Credentials.new(kms_providers[:kmip])
          end
          if kms_providers.key?(:local)
            @local = Local::Credentials.new(kms_providers[:local])
          end
          if @aws.nil? && @azure.nil? && @gcp.nil? && @kmip.nil? && @local.nil?
            raise ArgumentError.new(
              "KMS providers options must have one of the following keys: " +
              ":aws, :azure, :gcp, :kmip, :local"
            )
          end
        end

        # Convert credentials object to a BSON document in libmongocrypt format.
        #
        # @return [ BSON::Document ] Credentials as BSON document.
        def to_document
          BSON::Document.new.tap do |bson|
            bson[:aws] = @aws.to_document if @aws
            bson[:azure] = @azure.to_document if @azure
            bson[:gcp] = @gcp.to_document if @gcp
            bson[:kmip] = @kmip.to_document if @kmip
            bson[:local] = @local.to_document if @local
          end
        end
      end
    end
  end
end
