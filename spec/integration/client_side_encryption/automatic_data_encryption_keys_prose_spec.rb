# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Automatic Data Encryption Keys' do
    require_libmongocrypt
    require_enterprise
    require_topology :replica_set, :sharded, :load_balanced
    min_server_fcv '6.0'

    include_context 'define shared FLE helpers'

    let(:test_database_name) do
      'automatic_data_encryption_keys'
    end

    let(:key_vault_client) do
      ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses)
    end

    let(:client_encryption) do
      Mongo::ClientEncryption.new(
        key_vault_client,
        kms_tls_options: kms_tls_options,
        key_vault_namespace: key_vault_namespace,
        kms_providers: {
          local: {
            key: local_master_key
          },
          aws: {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: SpecConfig.instance.fle_aws_secret,
          }
        }
      )
    end

    let(:database) do
      authorized_client.use(test_database_name).database
    end

    before(:each) do
      authorized_client.use(key_vault_db)[key_vault_coll].drop
      authorized_client.use(test_database_name).database.drop
    end

    shared_examples 'creates data keys automatically' do
      it 'fails document validation when insert unencrypted value' do
        opts = {
          encrypted_fields: {
            fields: [{
              path: 'ssn',
              bsonType: 'string',
              keyId: nil
            }]
          }
        }
        client_encryption.create_encrypted_collection(
          database,
          'testing1',
          opts,
          kms_provider,
          master_key
        )
        expect do
          database['testing1'].insert_one(ssn: '123-45-6789')
        end.to raise_error(Mongo::Error::OperationFailure, /Document failed validation/)
      end

      it 'fails when missing encrypted field' do
        expect do
          client_encryption.create_encrypted_collection(
            database,
            'testing1',
            {},
            kms_provider,
            master_key
          )
        end.to raise_error(ArgumentError, /coll_opts must contain :encrypted_fields/)
      end

      it 'fails when invalid keyId provided' do
        opts = {
          encrypted_fields: {
            fields: [{
              path: 'ssn',
              bsonType: 'string',
              keyId: false
            }]
          }
        }
        expect do
          client_encryption.create_encrypted_collection(
            database,
            'testing1',
            opts,
            kms_provider,
            master_key
          )
        end.to raise_error(Mongo::Error::CryptError, /keyId' is the wrong type/)
      end

      it 'successfully inserts encrypted value' do
        opts = {
          encrypted_fields: {
            fields: [{
              path: 'ssn',
              bsonType: 'string',
              keyId: nil
            }]
          }
        }
        _, new_encrypted_fields = client_encryption.create_encrypted_collection(
          database,
          'testing1',
          opts,
          kms_provider,
          master_key
        )
        key_id = new_encrypted_fields[:fields].first[:keyId]
        encrypted_payload = client_encryption.encrypt(
          '123-45-6789',
          key_id: key_id,
          algorithm: 'Unindexed'
        )
        expect do
          database['testing1'].insert_one(ssn: encrypted_payload)
        end.not_to raise_error
      end
    end

    context 'aws' do
      let(:kms_provider) { 'aws' }
      let(:master_key) { { region: 'us-east-1', key: 'arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0' } }

      it_behaves_like 'creates data keys automatically'
    end

    context 'local' do
      let(:kms_provider) { 'local' }
      let(:master_key) { { key: local_master_key } }

      it_behaves_like 'creates data keys automatically'
    end
  end
end
