# frozen_string_literal: true

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Automatic Data Encryption Keys' do
    require_libmongocrypt
    require_enterprise
    require_topology :replica_set, :sharded, :load_balanced
    min_server_version '7.0.0-rc0'

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

    before do
      authorized_client.use(key_vault_db)[key_vault_coll].drop
      authorized_client.use(test_database_name).database.drop
    end

    shared_examples 'creates data keys automatically' do
      let(:opts) do
        { encrypted_fields: { fields: [ field ] } }
      end

      context 'when insert unencrypted value' do
        let(:field) do
          {
            path: 'ssn',
            bsonType: 'string',
            keyId: nil
          }
        end

        it 'fails document validation' do
          client_encryption.create_encrypted_collection(
            database, 'testing1', opts, kms_provider, master_key
          )
          expect { database['testing1'].insert_one(ssn: '123-45-6789') }
            .to raise_error(Mongo::Error::OperationFailure, /Document failed validation/)
        end
      end

      it 'fails when missing encrypted field' do
        expect do
          client_encryption.create_encrypted_collection(
            database, 'testing1', {}, kms_provider, master_key
          )
        end.to raise_error(ArgumentError, /coll_opts must contain :encrypted_fields/)
      end

      context 'when invalid keyId provided' do
        let(:field) do
          {
            path: 'ssn',
            bsonType: 'string',
            keyId: false
          }
        end

        it 'fails' do
          expect do
            client_encryption.create_encrypted_collection(
              database, 'testing1', opts, kms_provider, master_key
            )
          end.to raise_error(Mongo::Error::CryptError, /keyId' is the wrong type/)
        end
      end

      context 'when configured correctly' do
        let(:field) do
          {
            path: 'ssn',
            bsonType: 'string',
            keyId: nil
          }
        end

        let(:new_encrypted_fields) do
          _, new_encrypted_fields = client_encryption.create_encrypted_collection(
            database, 'testing1', opts, kms_provider, master_key
          )

          new_encrypted_fields
        end

        let(:key_id) do
          new_encrypted_fields[:fields].first[:keyId]
        end

        let(:encrypted_payload) do
          client_encryption.encrypt(
            '123-45-6789',
            key_id: key_id,
            algorithm: 'Unindexed'
          )
        end

        it 'successfully inserts encrypted value' do
          expect do
            database['testing1'].insert_one(ssn: encrypted_payload)
          end.not_to raise_error
        end
      end
    end

    context 'with aws' do
      let(:kms_provider) { 'aws' }
      let(:master_key) do
        {
          region: 'us-east-1',
          key: 'arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0'
        }
      end

      it_behaves_like 'creates data keys automatically'
    end

    context 'with local' do
      let(:kms_provider) { 'local' }
      let(:master_key) { { key: local_master_key } }

      it_behaves_like 'creates data keys automatically'
    end
  end
end
