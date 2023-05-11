# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: External Key Vault Test' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options
      )
    end

    let(:test_schema_map) do
      {
        'db.coll' => BSON::ExtJSON.parse(File.read('spec/support/crypt/external/external-schema.json'))
      }
    end

    let(:external_key_vault_client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          user: 'fake-user',
          password: 'fake-pwd'
        )
      )
    end

    let(:data_key_id) do
      BSON::Binary.new(Base64.decode64('LOCALAAAAAAAAAAAAAAAAA=='), :uuid)
    end

    before do
      client.use('keyvault')['datakeys'].drop
      client.use('db')['coll'].drop

      data_key = BSON::ExtJSON.parse(File.read('spec/support/crypt/external/external-key.json'))
      client.use('keyvault')['datakeys', write_concern: { w: :majority }].insert_one(data_key)
    end

    context 'with default key vault client' do
      let(:client_encrypted) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: local_kms_providers,
              key_vault_namespace: 'keyvault.datakeys',
              schema_map: test_schema_map,
              # Spawn mongocryptd on non-default port for sharded cluster tests
              extra_options: extra_options,
            },
            database: 'db',
          )
        )
      end

      let(:client_encryption) do
        Mongo::ClientEncryption.new(
          client,
          {
            kms_providers: local_kms_providers,
            key_vault_namespace: 'keyvault.datakeys',
          }
        )
      end

      it 'inserts an encrypted document with client' do
        result = client_encrypted['coll'].insert_one(encrypted: 'test')
        expect(result).to be_ok

        encrypted = client.use('db')['coll'].find.first['encrypted']
        expect(encrypted).to be_ciphertext
      end

      it 'encrypts a value with client encryption' do
        encrypted = client_encryption.encrypt(
          'test',
          {
            key_id: data_key_id,
            algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
          }
        )

        expect(encrypted).to be_ciphertext
      end
    end

    context 'with external key vault client' do
      let(:client_encrypted) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: local_kms_providers,
              key_vault_namespace: 'keyvault.datakeys',
              schema_map: test_schema_map,
              key_vault_client: external_key_vault_client,
              # Spawn mongocryptd on non-default port for sharded cluster tests
              extra_options: extra_options,
            },
            database: 'db',
          )
        )
      end

      let(:client_encryption) do
        Mongo::ClientEncryption.new(
          external_key_vault_client,
          {
            kms_providers: local_kms_providers,
            key_vault_namespace: 'keyvault.datakeys',
          }
        )
      end

       it 'raises an authentication exception when auto encrypting' do
        expect do
          client_encrypted['coll'].insert_one(encrypted: 'test')
        end.to raise_error(Mongo::Auth::Unauthorized, /fake-user/)
      end

      it 'raises an authentication exception when explicit encrypting' do
        expect do
          client_encryption.encrypt(
            'test',
            {
              key_id: data_key_id,
              algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
            }
          )
        end.to raise_error(Mongo::Auth::Unauthorized, /fake-user/)
      end
    end
  end
end
