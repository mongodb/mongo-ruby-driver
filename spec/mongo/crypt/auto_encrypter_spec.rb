require 'spec_helper'

describe Mongo::Crypt::AutoEncrypter do
  require_libmongocrypt
  min_server_fcv '4.2'
  require_enterprise
  clean_slate

  include_context 'define shared FLE helpers'

  let(:auto_encrypter) do
    described_class.new(
      auto_encryption_options.merge(
        client: authorized_client.use(:auto_encryption),
        # Spawn mongocryptd on non-default port for sharded cluster tests
        extra_options: extra_options
      )
    )
  end

  let(:client) { authorized_client }

  let(:db_name) { 'auto_encryption' }
  let(:collection_name) { 'users' }

  let(:command) do
    {
      'insert' => collection_name,
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
      users_collection = client.use(db_name)[collection_name]
      users_collection.drop
      client.use(db_name)[collection_name,
        {
          'validator' => { '$jsonSchema' => schema_map }
        }
      ].create
    end
  end

  shared_context 'without jsonSchema validator' do
    before do
      users_collection = client.use(db_name)[collection_name]
      users_collection.drop
      users_collection.create
    end
  end

  shared_examples 'a functioning auto encrypter' do
    describe '#encrypt' do
      it 'replaces the ssn field with a BSON::Binary' do
        result = auto_encrypter.encrypt(db_name, command)
        expect(result).to eq(encrypted_command)
      end
    end

    describe '#decrypt' do
      it 'returns the unencrypted document' do
        result = auto_encrypter.decrypt(encrypted_command)
        expect(result).to eq(command)
      end
    end
  end

  before do
    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)
  end

  after do
    auto_encrypter.close
  end

  context 'with schema map in auto encryption commands' do
    include_context 'without jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        key_vault_namespace: key_vault_namespace,
        schema_map: { "#{db_name}.#{collection_name}": schema_map },
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
          result = auto_encrypter.encrypt(db_name, command)
          expect(result).to eq(command)
        end
      end

      describe '#decrypt' do
        it 'still performs decryption' do
          result = auto_encrypter.decrypt(encrypted_command)
          expect(result).to eq(command)
        end
      end
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'

      describe '#encrypt' do
        it 'does not perform encryption' do
          result = auto_encrypter.encrypt(db_name, command)
          expect(result).to eq(command)
        end
      end

      describe '#decrypt' do
        it 'still performs decryption' do
          result = auto_encrypter.decrypt(encrypted_command)
          expect(result).to eq(command)
        end
      end
    end
  end
end
