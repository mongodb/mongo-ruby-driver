# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2009-2020 MongoDB Inc.
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
  LOCAL_MASTER_KEY_B64 = 'Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3' +
  'YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk'.freeze

  LOCAL_MASTER_KEY = Base64.decode64(LOCAL_MASTER_KEY_B64)

  # For all FLE-related tests
  shared_context 'define shared FLE helpers' do
    # 96-byte binary string, base64-encoded local master key
    let(:local_master_key_b64) do
      Crypt::LOCAL_MASTER_KEY_B64
    end

    let(:local_master_key) { Crypt::LOCAL_MASTER_KEY }

    # Data key id as a binary string
    let(:key_id) { data_key['_id'] }

    # Data key alternate name
    let(:key_alt_name) { 'ssn_encryption_key' }

    # Deterministic encryption algorithm
    let(:algorithm) { 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic' }

    # Local KMS provider options
    let(:local_kms_providers) { { local: { key: local_master_key } } }

    # AWS KMS provider options
    let(:aws_kms_providers) do
      {
        aws: {
          access_key_id: SpecConfig.instance.fle_aws_key,
          secret_access_key: SpecConfig.instance.fle_aws_secret,
        }
      }
    end

    # Azure KMS provider options
    let(:azure_kms_providers) do
      {
        azure: {
          tenant_id: SpecConfig.instance.fle_azure_tenant_id,
          client_id: SpecConfig.instance.fle_azure_client_id,
          client_secret: SpecConfig.instance.fle_azure_client_secret,
        }
      }
    end

    let(:gcp_kms_providers) do
      {
        gcp: {
          email: SpecConfig.instance.fle_gcp_email,
          private_key: SpecConfig.instance.fle_gcp_private_key,
        }
      }
    end

    let(:kmip_kms_providers) do
      {
        kmip: {
          endpoint: SpecConfig.instance.fle_kmip_endpoint,
        }
      }
    end

    # Key vault database and collection names
    let(:key_vault_db) { 'keyvault' }
    let(:key_vault_coll) { 'datakeys' }
    let(:key_vault_namespace) { "#{key_vault_db}.#{key_vault_coll}" }

    # Example value to encrypt
    let(:ssn) { '123-456-7890' }

    let(:key_vault_collection) do
      authorized_client.with(
        database: key_vault_db,
        write_concern: { w: :majority }
      )[key_vault_coll]
    end

    let(:extra_options) do
      {
        mongocryptd_spawn_args: ["--port=#{SpecConfig.instance.mongocryptd_port}"],
        mongocryptd_uri: "mongodb://localhost:#{SpecConfig.instance.mongocryptd_port}",
      }
    end

    let(:kms_tls_options) do
      {}
    end

    let(:default_kms_tls_options_for_provider) do
      {
        ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file,
        ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
        ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
      }
    end

    let(:encrypted_fields) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/encrypted_fields/encryptedFields.json'))
    end

    %w[DecimalNoPrecision DecimalPrecision Date DoubleNoPrecision DoublePrecision Int Long].each do |type|
      let("range_encrypted_fields_#{type.downcase}".to_sym) do
        BSON::ExtJSON.parse(
          File.read("spec/support/crypt/encrypted_fields/range-encryptedFields-#{type}.json"),
          mode: :bson
        )
      end
    end

    let(:key1_document) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/keys/key1-document.json'))
    end
  end

  # For tests that require local KMS to be configured
  shared_context 'with local kms_providers' do
    let(:kms_provider_name) { 'local' }
    let(:kms_providers) { local_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document_local.json'))
    end

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_local.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end

    let(:data_key_options) { {} }

    let(:encrypted_ssn) do
      "ASzggCwAAAAAAAAAAAAAAAAC/OvUvE0N5eZ5vhjcILtGKZlxovGhYJduEfsR\n7NiH68Ft" +
      "tXzHYqT0DKgvn3QjjTbS/4SPfBEYrMIS10Uzf9R1Ky4D5a19mYCp\nmv76Z8Rzdmo=\n"
    end
  end

  shared_context 'with local kms_providers and key alt names' do
    include_context 'with local kms_providers'

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_local_key_alt_names.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end
  end

  # For tests that require AWS KMS to be configured
  shared_context 'with AWS kms_providers' do
    before do
      unless SpecConfig.instance.fle_aws_key &&
        SpecConfig.instance.fle_aws_secret &&
        SpecConfig.instance.fle_aws_region &&
        SpecConfig.instance.fle_aws_arn

        reason = "This test requires the MONGO_RUBY_DRIVER_AWS_KEY, " +
                "MONGO_RUBY_DRIVER_AWS_SECRET, MONGO_RUBY_DRIVER_AWS_REGION, " +
                "MONGO_RUBY_DRIVER_AWS_ARN environment variables to be set information from AWS."

        if SpecConfig.instance.fle?
          fail(reason)
        else
          skip(reason)
        end
      end
    end

    let(:kms_provider_name) { 'aws' }
    let(:kms_providers) { aws_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document_aws.json'))
    end

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_aws.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end

    let(:data_key_options) do
      {
        master_key: {
          region: aws_region,
          key: aws_arn,
          endpoint: "#{aws_endpoint_host}:#{aws_endpoint_port}"
        }
      }
    end

    let(:aws_region) { SpecConfig.instance.fle_aws_region }
    let(:aws_arn) { SpecConfig.instance.fle_aws_arn }
    let(:aws_endpoint_host) { "kms.#{aws_region}.amazonaws.com" }
    let(:aws_endpoint_port) { 443 }

    let(:encrypted_ssn) do
      "AQFkgAAAAAAAAAAAAAAAAAACX/YG2ZOHWU54kARE17zDdeZzKgpZffOXNaoB\njmvdVa/" +
      "yTifOikvxEov16KxtQrnaKWdxQL03TVgpoLt4Jb28pqYKlgBj3XMp\nuItZpQeFQB4=\n"
    end
  end

  shared_context 'with AWS kms_providers and key alt names' do
    include_context 'with AWS kms_providers'

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_aws_key_alt_names.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end
  end

  shared_context 'with Azure kms_providers' do
    before do
      unless SpecConfig.instance.fle_azure_client_id &&
        SpecConfig.instance.fle_azure_client_secret &&
        SpecConfig.instance.fle_azure_tenant_id &&
        SpecConfig.instance.fle_azure_identity_platform_endpoint

        reason = 'This test requires the MONGO_RUBY_DRIVER_AZURE_TENANT_ID, ' +
        'MONGO_RUBY_DRIVER_AZURE_CLIENT_ID, MONGO_RUBY_DRIVER_AZURE_CLIENT_SECRET, ' +
        'MONGO_RUBY_DRIVER_AZURE_IDENTITY_PLATFORM_ENDPOINT environment variables to be set information from Azure.'

        if SpecConfig.instance.fle?
          fail(reason)
        else
          skip(reason)
        end
      end
    end

    let(:kms_provider_name) { 'azure' }
    let(:kms_providers) { azure_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document_azure.json'))
    end

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_azure.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end

    let(:data_key_options) do
      {
        master_key: {
          key_vault_endpoint: SpecConfig.instance.fle_azure_key_vault_endpoint,
          key_name: SpecConfig.instance.fle_azure_key_name,
        }
      }
    end

    let(:encrypted_ssn) do
      "AQGVERAAAAAAAAAAAAAAAAACFq9wVyHGWquXjaAjjBwI3MQNuyokz/+wWSi0\n8n9iu1cKzTGI9D5uVSNs64tBulnZpywtuewBQtJIphUoEr5YpSFLglOh3bp6\nmC9hfXSyFT4="
    end
  end

  shared_context 'with Azure kms_providers and key alt names' do
    include_context 'with Azure kms_providers'

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_azure_key_alt_names.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end
  end

  shared_context 'with GCP kms_providers' do
    before do
      unless SpecConfig.instance.fle_gcp_email &&
        SpecConfig.instance.fle_gcp_private_key &&
        SpecConfig.instance.fle_gcp_project_id &&
        SpecConfig.instance.fle_gcp_location &&
        SpecConfig.instance.fle_gcp_key_ring &&
        SpecConfig.instance.fle_gcp_key_name

        reason = 'This test requires the MONGO_RUBY_DRIVER_GCP_EMAIL, ' +
        'MONGO_RUBY_DRIVER_GCP_PRIVATE_KEY, ' +
        'MONGO_RUBY_DRIVER_GCP_PROJECT_ID, MONGO_RUBY_DRIVER_GCP_LOCATION, ' +
        'MONGO_RUBY_DRIVER_GCP_KEY_RING, MONGO_RUBY_DRIVER_GCP_KEY_NAME ' +
        'environment variables to be set information from GCP.'

        if SpecConfig.instance.fle?
          fail(reason)
        else
          skip(reason)
        end
      end
    end

    let(:kms_provider_name) { 'gcp' }
    let(:kms_providers) { gcp_kms_providers }

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document_gcp.json'))
    end

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_gcp.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end

    let(:data_key_options) do
      {
        master_key: {
          project_id: SpecConfig.instance.fle_gcp_project_id,
          location: SpecConfig.instance.fle_gcp_location,
          key_ring: SpecConfig.instance.fle_gcp_key_ring,
          key_name: SpecConfig.instance.fle_gcp_key_name,
        }
      }
    end

    let(:encrypted_ssn) do
      "ARgjwAAAAAAAAAAAAAAAAAACxH7FeQ7bsdbcs8uiNn5Anj2MAU7eS5hFiQsH\nYIEMN88QVamaAgiE+EIYHiRMYGxUFaaIwD17tjzZ2wyQbDd1qMO9TctkIFzn\nqQTOP6eSajU="
    end
  end

  shared_context 'with GCP kms_providers and key alt names' do
    include_context 'with GCP kms_providers'

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_gcp_key_alt_names.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end
  end

  shared_context 'with KMIP kms_providers' do
    let(:kms_provider_name) { 'kmip' }
    let(:kms_providers) { kmip_kms_providers }

    let(:kms_tls_options) do
      {
        kmip: default_kms_tls_options_for_provider
      }
    end

    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document_kmip.json'))
    end

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_kmip.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end

    let(:data_key_options) do
      {
        master_key: {
          key_id: "1"
        }
      }
    end

    let(:encrypted_ssn) do
      "ASjCDwAAAAAAAAAAAAAAAAAC/ga87lE2+z1ZVpLcoP51EWKVgne7f5/vb0Jq\nt3odeB0IIuoP7xxLCqSJe+ueFm86gVA1gIiip5CKe/043PD4mquxO2ARwy8s\nCX/D4tMmvDA="
    end
  end

  shared_context 'with KMIP kms_providers and key alt names' do
    include_context 'with KMIP kms_providers'

    let(:schema_map_file_path) do
      'spec/support/crypt/schema_maps/schema_map_kmip_key_alt_names.json'
    end

    let(:schema_map) do
      BSON::ExtJSON.parse(File.read(schema_map_file_path))
    end
  end
end
