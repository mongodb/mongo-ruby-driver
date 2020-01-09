require 'spec_helper'
require 'json'

describe 'Auto Encryption' do
  require_libmongocrypt
  require_enterprise

  let(:auto_encryption_options) do
    {
      kms_providers: { local: { key: "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk" } },
      key_vault_namespace: 'admin.datakeys',
      schema_map: schema_map,
      bypass_auto_encryption: bypass_auto_encryption
    }
  end

  let(:encryption_client) do
    new_local_client(
      'mongodb://localhost:27017/test',
      {
        auto_encryption_options: auto_encryption_options.merge(mongocryptd_server_selection_timeout: 3)
      }
    )
  end

  let(:client) do
    new_local_client('mongodb://localhost:27017')
  end

  let(:ssn) { '123-456-7890' }
  let(:bypass_auto_encryption) { false }
  let(:schema_map) { { "test.users" => json_schema } }

  let(:encrypted_ssn) do
    BSON::Binary.new(Base64.decode64("ASzggCwAAAAAAAAAAAAAAAAC/OvUvE0N5eZ5vhjcILtGKZlxovGhYJduEfsR\n7NiH68FttXzHYqT0DKgvn3QjjTbS/4SPfBEYrMIS10Uzf9R1Ky4D5a19mYCp\nmv76Z8Rzdmo=\n"), :ciphertext)
  end

  let(:local_data_key) do
    Utils.parse_extended_json(JSON.parse(File.read('spec/mongo/crypt/data/key_document.json')))
  end

  let(:json_schema) do
    Utils.parse_extended_json(JSON.parse(File.read('spec/mongo/crypt/data/schema_map.json')))
  end

  before(:each) do
    client.use(:test)[:users].drop
    client[:datakeys].drop
    client[:datakeys].insert_one(local_data_key)
  end

  describe '#insert_one' do
    context 'with validator' do
      let(:schema_map) { nil }

      before do
        client.use(:test)[:users,
          {
            'validator' => { '$jsonSchema' => json_schema }
          }
        ].create
      end

      it 'encrypts the command' do
        result = encryption_client[:users].insert_one({ ssn: ssn })
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client.use(:test)[:users].find(_id: id).first
        expect(document['ssn']).to eq(encrypted_ssn)
      end
    end

    context 'with schema map' do
      it 'encrypts the command' do
        result = encryption_client[:users].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client.use(:test)[:users].find(_id: id).first
        expect(document['ssn']).to eq(encrypted_ssn)
      end
    end

    context 'with bypass_auto_encryption=true' do
      let(:bypass_auto_encryption) { true }

      it 'does not encrypt the command' do
        result = encryption_client[:users].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client.use(:test)[:users].find(_id: id).first
        expect(document['ssn']).to eq(ssn)
      end
    end
  end
end
