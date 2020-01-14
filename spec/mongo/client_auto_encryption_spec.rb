require 'spec_helper'

describe Mongo::Client do
  require_libmongocrypt
  require_enterprise
  clean_slate

  let(:schema_map) { BSON::ExtJSON.parse(File.read('spec/mongo/crypt/data/schema_map.json')) }
  let(:key) { "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk" }

  let(:client) { new_local_client('mongodb://localhost:27017') }

  let(:encryption_client) do
    new_local_client(
      'mongodb://localhost:27017/test',
      { auto_encryption_options: auto_encryption_options.merge(mongocryptd_server_selection_timeout: 3) }
    )
  end

  let(:data_key) do
    BSON::ExtJSON.parse(File.read('spec/mongo/crypt/data/key_document.json'))
  end

  let(:ssn) { '123-456-7890' }

  let(:command) do
    {
      'insert' => 'users',
      'ordered' => true,
      'lsid' => {
        'id' => BSON::Binary.new(Base64.decode64("CzgjT+byRK+FKUWG6QbyjQ==\n"), :uuid)
      },
      'documents' => [
        {
          'ssn' => '123-456-7890',
          '_id' => BSON::ObjectId('5e16516e781d8a89b94df6df')
        }
      ]
    }
  end

  let(:encrypted_command) do
    command.merge(
      'documents' => [
        {
          'ssn' => BSON::Binary.new(Base64.decode64("ASzggCwAAAAAAAAAAAAAAAAC/OvUvE0N5eZ5vhjcILtGKZlxovGhYJduEfsR\n7NiH68FttXzHYqT0DKgvn3QjjTbS/4SPfBEYrMIS10Uzf9R1Ky4D5a19mYCp\nmv76Z8Rzdmo=\n"), :ciphertext),
          '_id' => BSON::ObjectId('5e16516e781d8a89b94df6df')
        }
      ]
    )
  end

  shared_context 'with jsonSchema validator' do
    before do
      users_collection = client.use(:test)[:users]
      users_collection.drop
      client.use(:test)[:users,
        {
          'validator' => { '$jsonSchema' => schema_map }
        }
      ].create
    end
  end

  shared_context 'without jsonSchema validator' do
    before do
      users_collection = client.use(:test)[:users]
      users_collection.drop
      users_collection.create
    end
  end

  before do
    key_vault_collection = client.use(:admin)[:datakeys]
    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)
  end

  context 'with schema map in auto encryption commands' do
    include_context 'without jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: { local: { key: key } },
        key_vault_namespace: 'admin.datakeys',
        schema_map: { 'test.users': schema_map }
      }
    end

    describe '#encrypt' do
      it 'replaces the ssn field with a BSON::Binary' do
        result = encryption_client.encrypt('test', command)
        expect(result).to eq(encrypted_command)
      end
    end

    describe '#decrypt' do
      it 'returns the unencrypted document' do
        result = encryption_client.decrypt(encrypted_command)
        expect(result).to eq(command)
      end
    end
  end

  context 'with schema map collection validator' do
    include_context 'with jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: { local: { key: key } },
        key_vault_namespace: 'admin.datakeys'
      }
    end

    describe '#encrypt' do
      it 'replaces the ssn field with a BSON::Binary' do
        result = encryption_client.encrypt('test', command)
        expect(result).to eq(encrypted_command)
      end
    end

    describe '#decrypt' do
      it 'returns the unencrypted document' do
        result = encryption_client.decrypt(encrypted_command)
        expect(result).to eq(command)
      end
    end
  end

  context 'with no validator or client option' do
    include_context 'without jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: { local: { key: key } },
        key_vault_namespace: 'admin.datakeys',
      }
    end

    describe '#encrypt' do
      it 'does not perform encryption' do
        result = encryption_client.encrypt('test', command)
        expect(result).to eq(command)
      end
    end

    describe '#decrypt' do
      it 'still performs decryption' do
        result = encryption_client.decrypt(encrypted_command)
        expect(result).to eq(command)
      end
    end
  end
end
