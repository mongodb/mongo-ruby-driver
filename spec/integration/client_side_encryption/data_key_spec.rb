require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: Data key and double encryption' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    let(:subscriber) { EventSubscriber.new }

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    let(:test_schema_map) do
      {
        "db.coll": {
          "bsonType": "object",
          "properties": {
            "encrypted_placeholder": {
              "encrypt": {
                "keyId": "/placeholder",
                "bsonType": "string",
                "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
              }
            }
          }
        }
      }
    end

    let(:client_encrypted) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: {
              local: { key: local_master_key },
              aws: {
                access_key_id: SpecConfig.instance.fle_aws_key,
                secret_access_key: SpecConfig.instance.fle_aws_secret,
              },
            },
            key_vault_namespace: 'admin.datakeys',
            schema_map: test_schema_map,
          },
          database: :db,
        )
      )
    end

    let(:client_encryption) do
      Mongo::ClientEncryption.new(
        client,
        {
          kms_providers: {
            local: { key: local_master_key },
            aws: {
              access_key_id: SpecConfig.instance.fle_aws_key,
              secret_access_key: SpecConfig.instance.fle_aws_secret,
            }
          },
          key_vault_namespace: 'admin.datakeys',
        },
      )
    end

    before do
      client.use(:admin)[:datakeys].drop
      client.use(:db)[:coll].drop
    end

    shared_examples 'can create and use a data key' do
      it 'creates a data key and uses it for encryption' do
        data_key_id = client_encryption.create_data_key(
          kms_provider_name,
          data_key_options.merge(key_alt_names: [key_alt_name])
        )

        expect(data_key_id).to be_uuid

        keys = client.use(:admin)[:datakeys].find(_id: data_key_id)

        expect(keys.count).to eq(1)
        expect(keys.first['masterKey']['provider']).to eq(kms_provider_name)

        command_started_event = subscriber.started_events.find do |event|
          event.command_name == 'find'
        end

        expect(command_started_event).not_to be_nil

        encrypted = client_encryption.encrypt(
          value_to_encrypt,
          {
            key_id: data_key_id,
            algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
          }
        )

        expect(encrypted).to be_ciphertext

        client_encrypted[:coll].insert_one(
          _id: kms_provider_name,
          'value': encrypted,
        )

        document = client_encrypted[:coll].find(_id: kms_provider_name).first

        expect(document['value']).to eq(value_to_encrypt)

        encrypted_with_alt_name = client_encryption.encrypt(
          value_to_encrypt,
          {
            key_alt_name: key_alt_name,
            algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
          }
        )

        expect(encrypted_with_alt_name).to be_ciphertext
        expect(encrypted_with_alt_name).to eq(encrypted)

        expect do
          client_encrypted[:coll].insert_one(encrypted_placeholder: encrypted)
        end.to raise_error(Mongo::Error::OperationFailure, /Cannot encrypt element of type binData/)
      end
    end

    context 'with local KMS options' do
      include_context 'with local kms_providers'

      let(:key_alt_name) { 'local_altname' }
      let(:data_key_options) { {} }
      let(:value_to_encrypt) { 'hello local' }

      it_behaves_like 'can create and use a data key'
    end

    context 'with AWS KMS options' do
      include_context 'with AWS kms_providers'

      let(:key_alt_name) { 'aws_altname' }
      let(:value_to_encrypt) { 'hello aws' }
      let(:data_key_options) do
        {
          master_key: {
            region: "us-east-1",
            key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
          }
        }
      end

      it_behaves_like 'can create and use a data key'
    end
  end
end
