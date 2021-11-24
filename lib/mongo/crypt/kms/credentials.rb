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
      class Credentials
        attr_reader :aws

        attr_reader :azure

        attr_reader :local

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
          if kms_providers.key?(:local)
            @local = Local::Credentials.new(kms_providers[:local])
          end
          if @aws.nil? && @azure.nil? && @local.nil?
            raise ArgumentError.new("KMS providers options must have one of the following keys: :aws, :azure, :local")
          end
        end

        def to_document
          BSON::Document.new({}).tap do |bson|
            bson[:aws] = @aws.to_document if @aws
            bson[:azure] = @azure.to_document if @azure
            bson[:local] = @local.to_document if @local
          end
        end
      end
    end
  end
end

require 'mongo/crypt/kms/aws.rb'
require 'mongo/crypt/kms/azure.rb'
require 'mongo/crypt/kms/local.rb'