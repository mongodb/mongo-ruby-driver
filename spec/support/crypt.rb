# Copyright (C) 2009-2019 MongoDB, Inc.
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

module Crypt

  # For tests that require local KMS to be configured
  shared_context 'with local kms_providers' do
    let(:kms_provider) { 'local' }

    let(:kms_providers) { local_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/mongo/crypt/data/key_document_local.json'))
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read('spec/mongo/crypt/data/schema_map_local.json'))
    end
  end

  # For tests that require AWS KMS to be configured
  shared_context 'with AWS kms_providers' do
    let(:kms_provider) { 'aws' }

    let(:kms_providers) { aws_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/mongo/crypt/data/key_document_aws.json'))
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read('spec/mongo/crypt/data/schema_map_aws.json'))
    end
  end

  def self.included(context)
    # 96-byte binary string, base64-encoded
    context.let(:local_master_key) do
      "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3" +
        "YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"
    end

    context.let(:fle_aws_key) { ENV['MONGO_RUBY_DRIVER_AWS_KEY'] }
    context.let(:fle_aws_secret) { ENV['MONGO_RUBY_DRIVER_AWS_SECRET'] }

    context.let(:key_vault_db) { 'admin' }
    context.let(:key_vault_coll) { 'datakeys' }
    context.let(:key_vault_namespace) { "#{key_vault_db}.#{key_vault_coll}" }

    context.let(:local_kms_providers) { { local: { key: local_master_key } } }
    context.let(:aws_kms_providers) do
      {
        aws: {
          access_key_id: fle_aws_key,
          secret_access_key: fle_aws_secret
        }
      }
    end
  end
end
