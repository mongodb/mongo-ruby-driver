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
  # For all FLE-related tests
  shared_context 'define shared FLE helpers' do
    # 96-byte binary string, base64-encoded local master key
    let(:local_master_key) do
      "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3" +
        "YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"
    end

    # Data key id as a binary string
    let(:key_id) { data_key['_id'].data }

    # Deterministic encryption algorithm
    let(:algorithm) { 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic' }

    # Local KMS provider options
    let(:local_kms_providers) { { local: { key: local_master_key } } }

    # AWS KMS provider options
    let(:aws_kms_providers) do
      {
        aws: {
          access_key_id: SpecConfig.instance.fle_aws_key,
          secret_access_key: SpecConfig.instance.fle_aws_secret
        }
      }
    end

    # Key vault database and collection names
    let(:key_vault_db) { 'admin' }
    let(:key_vault_coll) { 'datakeys' }
    let(:key_vault_namespace) { "#{key_vault_db}.#{key_vault_coll}" }
  end

  # For tests that require local KMS to be configured
  shared_context 'with local kms_providers' do
    let(:kms_provider_name) { 'local' }
    let(:kms_providers) { local_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document_local.json'))
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/schema_maps/schema_map_local.json'))
    end
  end

  # For tests that require AWS KMS to be configured
  shared_context 'with AWS kms_providers' do
    before do
      unless SpecConfig.instance.fle_aws_key && SpecConfig.instance.fle_aws_secret
        skip(
          'This test requires the MONGO_RUBY_DRIVER_AWS_KEY and ' +
          'MONGO_RUBY_DRIVER_AWS_SECRET environment variables to be set with ' +
          'AWS IAM credentials.'
        )
      end
    end

    let(:kms_provider_name) { 'aws' }
    let(:kms_providers) { aws_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document_aws.json'))
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/schema_maps/schema_map_aws.json'))
    end
  end
end
