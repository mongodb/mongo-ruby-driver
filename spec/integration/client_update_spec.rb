# frozen_string_literal: true
# rubocop:todo all

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
      authorized_client.use(:keyvault)[:datakeys, write_concern: { w: :majority }].drop
      authorized_client.use(:keyvault)[:datakeys, write_concern: { w: :majority }].insert_one(data_key)
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
              # Spawn mongocryptd on non-default port for sharded cluster tests
              extra_options: extra_options,
            },
            database: :auto_encryption
          ),
        )
      end

      context 'with new, invalid auto_encryption_options' do
        let(:new_auto_encryption_options) { { kms_providers: nil } }

        let(:new_client) do
          old_client.with(auto_encryption_options: new_auto_encryption_options)
        end

        # Detection of leaked background threads only, these tests do not
        # actually require a clean slate. https://jira.mongodb.org/browse/RUBY-2138
        clean_slate

        before do
          authorized_client.reconnect if authorized_client.closed?
        end

        it 'raises an exception' do
          expect do
            new_client
          end.to raise_error(ArgumentError)
        end

        it 'allows the original client to keep encrypting' do
          old_client[:users].insert_one(ssn: ssn)
          document = authorized_client.use(:auto_encryption)[:users].find.first
          expect(document['ssn']).to be_ciphertext
        end
      end

      context 'with new auto_encryption_options' do
        let!(:new_client) do
          old_client.with(auto_encryption_options: new_auto_encryption_options)
        end

        let(:new_auto_encryption_options) do
          {
            kms_providers: kms_providers,
            key_vault_namespace: key_vault_namespace,
            schema_map: { 'auto_encryption.users' => schema_map },
            # Spawn mongocryptd on non-default port for sharded cluster tests
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
        let!(:new_client) do
          old_client.with(auto_encryption_options: new_auto_encryption_options)
        end

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
              # Spawn mongocryptd on non-default port for sharded cluster tests
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
