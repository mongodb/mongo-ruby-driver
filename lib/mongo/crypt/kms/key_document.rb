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
      class KeyDocument

        KMS_PROVIDERS = %w(aws azure local).freeze

        def initialize(kms_provider, options)
          if options.nil?
            raise ArgumentError.new('Key document options must not be nil')
          end
          master_key = options[:master_key]
          @key_document = case kms_provider.to_s
            when 'aws' then KMS::AWS::KeyDocument.new(master_key)
            when 'azure' then KMS::Azure::KeyDocument.new(master_key)
            when 'local' then KMS::Local::KeyDocument.new(master_key)
            else
              raise ArgumentError.new("KMS provider must be one of #{KMS_PROVIDERS}")
          end
        end

        def to_document
          @key_document.to_document
        end
      end
    end
  end
end

require 'mongo/crypt/kms/aws.rb'
require 'mongo/crypt/kms/azure.rb'
require 'mongo/crypt/kms/local.rb'