# frozen_string_literal: true
# encoding: utf-8

require "spec_helper"

describe "RewrapManyDataKey" do
  require_libmongocrypt
  min_server_version "6.0.0"
  require_topology :replica_set, :sharded, :load_balanced

  include_context "define shared FLE helpers"

  let(:kms_providers) do
    {}.merge(aws_kms_providers)
      .merge(azure_kms_providers)
      .merge(gcp_kms_providers)
      .merge(kmip_kms_providers)
      .merge(local_kms_providers)
  end

  let(:master_keys) do
    {
      aws: {
        region: "us-east-1",
        key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
      },
      azure: {
        key_vault_endpoint: "key-vault-csfle.vault.azure.net",
        key_name: "key-name-csfle",
      },
      gcp: {
        project_id: "devprod-drivers",
        location: "global",
        key_ring: "key-ring-csfle",
        key_name: "key-name-csfle",
      },
      kmip: {}
    }
  end

  before(:each) do
    authorized_client.use('keyvault')['datakeys'].drop
  end

  %i(aws azure gcp kmip local).each do |src_provider|
    %i(aws azure gcp kmip local).each do |dst_provider|
      context "with #{src_provider} as source provider and #{dst_provider} as destination provider" do
        let(:client_encryption_1) do
          key_vault_client = ClientRegistry.instance.new_local_client(
            SpecConfig.instance.addresses,
            SpecConfig.instance.test_options
          )
          Mongo::ClientEncryption.new(
            key_vault_client,
            key_vault_namespace: 'keyvault.datakeys',
            kms_providers: kms_providers,
            kms_tls_options: {
              kmip: default_kms_tls_options_for_provider
            },
          )
        end

        let(:client_encryption_2) do
          key_vault_client = ClientRegistry.instance.new_local_client(
            SpecConfig.instance.addresses,
            SpecConfig.instance.test_options
          )
          Mongo::ClientEncryption.new(
            key_vault_client,
            key_vault_namespace: 'keyvault.datakeys',
            kms_providers: kms_providers,
            kms_tls_options: {
              kmip: default_kms_tls_options_for_provider
            },
          )
        end

        it 'rewraps' do
          key_id = client_encryption_1.create_data_key(
            src_provider.to_s,
            master_key: master_keys[src_provider]
          )
          ciphertext = client_encryption_1.encrypt(
            "test",
            key_id: key_id,
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
          )
          client_encryption_2.rewrap_many_data_key(
            {},
            provider: dst_provider.to_s,
            master_key: master_keys[dst_provider]
          )
          expect(client_encryption_1.decrypt(ciphertext)).to eq('test')
          expect(client_encryption_2.decrypt(ciphertext)).to eq('test')
        end
      end
    end
  end
end
