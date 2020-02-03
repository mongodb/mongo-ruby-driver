require 'spec_helper'

describe Mongo::Client do
  require_libmongocrypt
  require_enterprise
  clean_slate

  include_context 'define shared FLE helpers'

  let(:client) { authorized_client }

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      { auto_encryption_options: auto_encryption_options }
    ).use(db)
  end

  let(:db) { 'test'}
  let(:coll) { 'users' }

  let(:command) do
    {
      'insert' => coll,
      'ordered' => true,
      'lsid' => {
        'id' => BSON::Binary.new(Base64.decode64("CzgjT+byRK+FKUWG6QbyjQ==\n"), :uuid)
      },
      'documents' => [
        {
          'ssn' => ssn,
          '_id' => BSON::ObjectId('5e16516e781d8a89b94df6df')
        }
      ]
    }
  end

  let(:encrypted_command) do
    command.merge(
      'documents' => [
        {
          'ssn' => BSON::Binary.new(Base64.decode64(encrypted_ssn), :ciphertext),
          '_id' => BSON::ObjectId('5e16516e781d8a89b94df6df')
        }
      ]
    )
  end

  shared_context 'with jsonSchema validator' do
    before do
      users_collection = client.use(db)[coll]
      users_collection.drop
      client.use(db)[coll,
        {
          'validator' => { '$jsonSchema' => schema_map }
        }
      ].create
    end
  end

  shared_context 'without jsonSchema validator' do
    before do
      users_collection = client.use(db)[coll]
      users_collection.drop
      users_collection.create
    end
  end

  shared_examples 'a functioning auto encrypter' do
    describe '#encrypt' do
      it 'replaces the ssn field with a BSON::Binary' do
        result = encryption_client.encrypt(db, command)
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

  before do
    key_vault_collection = client.use(key_vault_db)[key_vault_coll]
    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)
  end

  context 'with schema map in auto encryption commands' do
    include_context 'without jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        key_vault_namespace: key_vault_namespace,
        schema_map: { "#{db}.#{coll}": schema_map }
      }
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end
  end

  context 'with schema map collection validator' do
    include_context 'with jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        key_vault_namespace: key_vault_namespace
      }
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end
  end

  context 'with no validator or client option' do
    include_context 'without jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        key_vault_namespace: key_vault_namespace,
      }
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'

      describe '#encrypt' do
        it 'does not perform encryption' do
          result = encryption_client.encrypt(db, command)
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

    context 'with local KMS providers' do
      include_context 'with local kms_providers'

      describe '#encrypt' do
        it 'does not perform encryption' do
          result = encryption_client.encrypt(db, command)
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
end
