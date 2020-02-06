require 'spec_helper'
require 'bson'
require 'json'

describe 'Auto Encryption' do
  require_libmongocrypt
  require_enterprise

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: { local: { key: "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk" } },
          key_vault_namespace: 'admin.datakeys',
          schema_map: schema_map,
          bypass_auto_encryption: bypass_auto_encryption
        },
        database: 'auto-encryption'
      ),
    )
  end

  let(:client) do
    authorized_client.use('auto-encryption')
  end

  let(:ssn) { '123-456-7890' }

  let(:bypass_auto_encryption) { false }

  let(:encrypted_ssn) do
    BSON::Binary.new(Base64.decode64("ASzggCwAAAAAAAAAAAAAAAAC/OvUvE0N5eZ5vhjcILtGKZlxovGhYJduEfsR\n7NiH68FttXzHYqT0DKgvn3QjjTbS/4SPfBEYrMIS10Uzf9R1Ky4D5a19mYCp\nmv76Z8Rzdmo=\n"), :ciphertext)
  end

  let(:local_data_key) do
    BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document.json'))
  end

  let(:json_schema) do
    BSON::ExtJSON.parse(File.read('spec/support/crypt/schema_maps/schema_map.json'))
  end

  shared_context 'bypass auto encryption' do
    let(:schema_map) { { "auto-encryption.users" => json_schema } }
    let(:bypass_auto_encryption) { true }

    before do
      client[:users].create
    end
  end

  shared_context 'jsonSchema validator on collection' do
    let(:schema_map) { nil }

    before do
      client[:users,
        {
          'validator' => { '$jsonSchema' => json_schema }
        }
      ].create
    end
  end

  shared_context 'schema map in client options' do
    let(:schema_map) { { "auto-encryption.users" => json_schema } }

    before do
      client[:users].create
    end
  end

  before(:each) do
    client[:users].drop
    client.use(:admin)[:datakeys].drop
    client.use(:admin)[:datakeys].insert_one(local_data_key)
  end

  describe '#insert_one' do
    let(:client_collection) { client[:users] }

    context 'with validator' do
      include_context 'jsonSchema validator on collection'

      it 'encrypts the command' do
        result = encryption_client[:users].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client_collection.find(_id: id).first
        document.should_not be_nil
        expect(document['ssn']).to eq(encrypted_ssn)
      end
    end

    context 'with schema map' do
      include_context 'schema map in client options'

      it 'encrypts the command' do
        result = encryption_client[:users].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client[:users].find(_id: id).first
        expect(document['ssn']).to eq(encrypted_ssn)
      end
    end

    context 'with bypass_auto_encryption=true' do
      include_context 'bypass auto encryption'

      it 'does not encrypt the command' do
        result = encryption_client[:users].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client[:users].find(_id: id).first
        expect(document['ssn']).to eq(ssn)
      end
    end
  end

  describe '#find' do
    shared_context 'with encrypted ssn document' do
      before do
        client[:users].insert_one(ssn: encrypted_ssn)
      end
    end

    context 'with validator' do
      include_context 'jsonSchema validator on collection'
      include_context 'with encrypted ssn document'

      it 'encrypts the command and decrypts the response' do
        document = encryption_client[:users].find(ssn: ssn).first
        document.should_not be_nil
        expect(document['ssn']).to eq(ssn)
      end
    end

    context 'with schema map' do
      include_context 'schema map in client options'
      include_context 'with encrypted ssn document'

      it 'encrypts the command and decrypts the response' do
        document = encryption_client[:users].find(ssn: ssn).first
        expect(document['ssn']).to eq(ssn)
      end
    end

    context 'with bypass_auto_encryption=true' do
      include_context 'bypass auto encryption'
      include_context 'with encrypted ssn document'

      it 'finds nothing' do
        document = encryption_client[:users].find(ssn: ssn).first
        expect(document).to be_nil
      end
    end
  end
end
