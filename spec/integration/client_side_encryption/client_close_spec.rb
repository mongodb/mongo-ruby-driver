# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Auto encryption client' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  context 'after client is disconnected' do
    include_context 'define shared FLE helpers'
    include_context 'with local kms_providers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: kms_providers,
            key_vault_namespace: 'keyvault.datakeys',
            schema_map: { 'auto_encryption.users' => schema_map },
            # Spawn mongocryptd on non-default port for sharded cluster tests
            extra_options: extra_options,
          },
          database: 'auto_encryption',
        )
      )
    end

    shared_examples 'a non-functioning auto-encrypter' do
      it 'raises a client closed error' do
        expect do
          client['users'].insert_one(ssn: '000-000-0000')
        end.to raise_error(Mongo::Error::ClientClosed)
      end
    end

    context 'after performing operation with auto encryption' do
      before do
        key_vault_collection.drop
        key_vault_collection.insert_one(data_key)

        client['users'].insert_one(ssn: ssn)
        client.close
      end

      it_behaves_like 'a non-functioning auto-encrypter'
    end

    context 'after performing operation without auto encryption' do
      before do
        client['users'].insert_one(age: 23)
        client.close
      end

      it_behaves_like 'a non-functioning auto-encrypter'
    end
  end
end
