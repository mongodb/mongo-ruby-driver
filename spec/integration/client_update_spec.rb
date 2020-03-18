require 'spec_helper'

describe Mongo::Client do
  clean_slate

  context 'auto encryption options' do
    require_libmongocrypt
    min_server_fcv '4.2'
    require_enterprise

    include_context 'define shared FLE helpers'
    include_context 'with local kms_providers'

    before do
      authorized_client.use(:admin)[:datakeys].drop
      authorized_client.use(:admin)[:datakeys].insert_one(data_key)
      authorized_client.use(:auto_encryption)[:users].drop
      authorized_client.use(:auto_encryption)[:users,
        {
          'validator' => { '$jsonSchema' => schema_map }
        }
      ].create
    end

    describe '#with' do
      let(:old_client) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: kms_providers,
              key_vault_namespace: key_vault_namespace,
              extra_options: extra_options,
            },
            database: :auto_encryption
          ),
        )
      end

      let!(:new_client) do
        old_client.with(auto_encryption_options: new_auto_encryption_options)
      end

      context 'with new auto_encryption_options' do
        let(:new_auto_encryption_options) do
          {
            kms_providers: kms_providers,
            key_vault_namespace: key_vault_namespace,
            schema_map: { 'auto_encryption.users' => schema_map },
            extra_options: extra_options,
          }
        end

        it 'creates a new client' do
          expect(new_client).not_to eq(old_client)
        end

        it 'maintains the old client\'s auto encryption options' do
          expect(old_client.encrypter.options[:schema_map]).to be_nil
        end

        it 'updates the client\'s auto encryption options' do
          expect(new_client.encrypter.options[:schema_map]).to eq('auto_encryption.users' => schema_map)
        end

        it 'shares a cluster with the old client' do
          expect(old_client.cluster).to eq(new_client.cluster)
        end

        it 'allows the original client to keep encrypting' do
          old_client[:users].insert_one(ssn: ssn)
          document = authorized_client.use(:auto_encryption)[:users].find.first
          expect(document['ssn']).to be_ciphertext
        end

        it 'allows the new client to keep encrypting' do
          old_client[:users].insert_one(ssn: ssn)
          document = authorized_client.use(:auto_encryption)[:users].find.first
          expect(document['ssn']).to be_ciphertext
        end
      end

      context 'with nil auto_encryption_options' do
        let(:new_auto_encryption_options) { nil }

        it 'removes auto encryption options' do
          expect(new_client.encrypter).to be_nil
        end

        it 'allows original client to keep encrypting' do
          old_client[:users].insert_one(ssn: ssn)
          document = authorized_client.use(:auto_encryption)[:users].find.first
          expect(document['ssn']).to be_ciphertext
        end
      end
    end

    describe '#use' do
      let(:old_client) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: kms_providers,
              key_vault_namespace: key_vault_namespace,
              extra_options: extra_options,
            }
          )
        )
      end

      let(:new_client) do
        old_client.use(:auto_encryption)
      end

      it 'creates a new client with encryption enabled' do
        new_client[:users].insert_one(ssn: ssn)
        document = authorized_client.use(:auto_encryption)[:users].find.first
        expect(document['ssn']).to be_ciphertext
      end
    end
  end
end
