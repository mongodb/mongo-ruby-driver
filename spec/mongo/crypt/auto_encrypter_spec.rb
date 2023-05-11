# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'
require 'tempfile'

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
        extra_options: auto_encrypter_extra_options
      )
    )
  end

  let(:auto_encrypter_extra_options) do
    # Spawn mongocryptd on non-default port for sharded cluster tests
    extra_options
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

  describe '#initialize' do
    include_context 'with local kms_providers'

    let(:auto_encryption_options) do
      {
        kms_providers: local_kms_providers,
        key_vault_namespace: key_vault_namespace,
        schema_map: { "#{db_name}.#{collection_name}": schema_map },
      }
    end

    let(:auto_encrypter) do
      described_class.new(
        auto_encryption_options.merge(
          client: client,
          # Spawn mongocryptd on non-default port for sharded cluster tests
          extra_options: extra_options
        )
      )
    end

    context 'when client has an unlimited pool' do
      let(:client) do
        new_local_client_nmio(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            max_pool_size: 0,
            database: 'auto_encryption'
          ),
        )
      end

      it 'reuses the client as key_vault_client and metadata_client' do
        expect(auto_encrypter.key_vault_client).to eq(client)
        expect(auto_encrypter.metadata_client).to eq(client)
      end
    end

    context 'when client has a limited pool' do
      let(:client) do
        new_local_client_nmio(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            max_pool_size: 20,
            database: 'auto_encryption'
          ),
        )
      end

      it 'creates new client for key_vault_client and metadata_client' do
        expect(auto_encrypter.key_vault_client).not_to eq(client)
        expect(auto_encrypter.metadata_client).not_to eq(client)
      end
    end

    context 'when crypt shared library is available' do
      it 'does not create a mongocryptd client' do
        allow_any_instance_of(Mongo::Crypt::Handle).to receive(:"crypt_shared_lib_available?").and_return true
        expect(auto_encrypter.mongocryptd_client).to be_nil
      end
    end
  end

  shared_examples 'with schema map in auto encryption commands' do
    include_context 'without jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        kms_tls_options: kms_tls_options,
        key_vault_namespace: key_vault_namespace,
        schema_map: { "#{db_name}.#{collection_name}": schema_map },
      }
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with Azure KMS providers' do
      include_context 'with Azure kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with GCP KMS providers' do
      include_context 'with GCP kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with KMIP KMS providers' do
      include_context 'with KMIP kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end
  end

  shared_examples 'with schema map file in auto encryption commands' do
    include_context 'without jsonSchema validator'

    let(:schema_map_file) do
      file = Tempfile.new('schema_map.json')
      file.write(JSON.dump(
        {
          "#{db_name}.#{collection_name}" => schema_map
        }
      ))
      file.flush
      file
    end

    after do
      schema_map_file.close
    end

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        kms_tls_options: kms_tls_options,
        key_vault_namespace: key_vault_namespace,
        schema_map_path: schema_map_file.path
      }
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with Azure KMS providers' do
      include_context 'with Azure kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with GCP KMS providers' do
      include_context 'with GCP kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with KMIP KMS providers' do
      include_context 'with KMIP kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end
  end

  shared_examples 'with schema map collection validator' do
    include_context 'with jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        kms_tls_options: kms_tls_options,
        key_vault_namespace: key_vault_namespace
      }
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with Azure KMS providers' do
      include_context 'with Azure kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with GCP KMS providers' do
      include_context 'with GCP kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with GCP KMS providers and PEM key' do
      require_mri

      include_context 'with GCP kms_providers'

      let(:kms_providers) do
        {
          gcp: {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: OpenSSL::PKey.read(
              Base64.decode64(SpecConfig.instance.fle_gcp_private_key)
            ).export,
          }
        }
      end

      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with KMIP KMS providers' do
      include_context 'with KMIP kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning auto encrypter'
    end
  end

  shared_examples 'with no validator or client option' do
    include_context 'without jsonSchema validator'

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        kms_tls_options: kms_tls_options,
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

    context 'with Azure KMS providers' do
      include_context 'with Azure kms_providers'

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

    context 'with GCP KMS providers' do
      include_context 'with GCP kms_providers'

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

    context 'with KMIP KMS providers' do
      include_context 'with KMIP kms_providers'

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

  context 'when using crypt shared library' do
    min_server_version '6.0.0'

    let(:auto_encrypter_extra_options) do
      {
        crypt_shared_lib_path: SpecConfig.instance.crypt_shared_lib_path
      }
    end

    let(:auto_encryption_options) do
      {
        kms_providers: kms_providers,
        kms_tls_options: kms_tls_options,
        key_vault_namespace: key_vault_namespace,
        schema_map: { "#{db_name}.#{collection_name}": schema_map },
      }
    end

    it_behaves_like 'with schema map in auto encryption commands'
    it_behaves_like 'with schema map file in auto encryption commands'
    it_behaves_like 'with schema map collection validator'
    it_behaves_like 'with no validator or client option'
  end

  context 'when using mongocryptd' do
    it_behaves_like 'with schema map in auto encryption commands'
    it_behaves_like 'with schema map file in auto encryption commands'
    it_behaves_like 'with schema map collection validator'
    it_behaves_like 'with no validator or client option'
  end
end
