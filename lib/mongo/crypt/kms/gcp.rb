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
      module GCP
        # GCP Cloud Key Management Credentials object contains credentials for
        # using GCP KMS provider.
        #
        # @api private
        class Credentials
          extend Forwardable
          include KMS::Validations

          # @return [ String ] GCP email to authenticate with.
          attr_reader :email

          # @return [ String ] GCP private key, base64 encoded DER format.
          attr_reader :private_key

          # @return [ String | nil ] GCP KMS endpoint.
          attr_reader :endpoint

          # @api private
          def_delegator :@opts, :empty?

          FORMAT_HINT = "GCP KMS provider options must be in the format: " +
              "{ email: 'EMAIL', private_key: 'PRIVATE-KEY' }"

          # Creates an GCP KMS credentials object form a parameters hash.
          #
          # @param [ Hash ] opts A hash that contains credentials for
          #   GCP KMS provider
          # @option opts [ String ] :email GCP email.
          # @option opts [ String ] :private_key GCP private key. This method accepts
          #   private key in either base64 encoded DER format, or PEM format.
          # @option opts [ String | nil ] :endpoint GCP endpoint, optional.
          #
          # @raise [ ArgumentError ] If required options are missing or incorrectly
          #   formatted.
          def initialize(opts)
            @opts = opts
            return if empty?

            @email = validate_param(:email, opts, FORMAT_HINT)
            @private_key = begin
              private_key_opt = validate_param(:private_key, opts, FORMAT_HINT)
              if BSON::Environment.jruby?
                # We cannot really validate private key on JRuby, so we assume
                # it is in base64 encoded DER format.
                private_key_opt
              else
                # Check if private key is in PEM format.
                pkey = OpenSSL::PKey::RSA.new(private_key_opt)
                # PEM it is, need to be converted to base64 encoded DER.
                der = if pkey.respond_to?(:private_to_der)
                  pkey.private_to_der
                else
                  pkey.to_der
                end
                Base64.encode64(der)
              end
            rescue OpenSSL::PKey::RSAError
              # Check if private key is in DER.
              begin
                OpenSSL::PKey.read(Base64.decode64(private_key_opt))
                # Private key is fine, use it.
                private_key_opt
              rescue OpenSSL::PKey::PKeyError
                raise ArgumentError.new(
                  "The private_key option must be either either base64 encoded DER format, or PEM format."
                )
              end
            end

            @endpoint = validate_param(
              :endpoint, opts, FORMAT_HINT, required: false
            )
          end

          # Convert credentials object to a BSON document in libmongocrypt format.
          #
          # @return [ BSON::Document ] Azure KMS credentials in libmongocrypt format.
          def to_document
            return BSON::Document.new if empty?
            BSON::Document.new({
              email: email,
              privateKey: BSON::Binary.new(private_key, :generic),
            }).tap do |bson|
              unless endpoint.nil?
                bson.update({ endpoint: endpoint })
              end
            end
          end
        end

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
