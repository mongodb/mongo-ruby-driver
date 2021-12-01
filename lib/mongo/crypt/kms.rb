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
      # This module contains helper methods for validating KMS parameters.
      #
      # @api private
      module Validations
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
        def validate_param(key, opts, format_hint, required: true)
          value = opts.fetch(key)
          return nil if value.nil? && !required
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
    end
  end
end

require "mongo/crypt/kms/credentials"
require "mongo/crypt/kms/master_key_document"
require 'mongo/crypt/kms/aws'
require 'mongo/crypt/kms/azure'
require 'mongo/crypt/kms/gcp'
require 'mongo/crypt/kms/local'
