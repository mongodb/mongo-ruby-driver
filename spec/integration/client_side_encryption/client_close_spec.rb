# frozen_string_literal: true
# rubocop:todo all

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

    shared_examples 'a functioning auto-encrypter' do
      it 'can still perform encryption' do
        result = client['users'].insert_one(ssn: '000-000-0000')
        expect(result).to be_ok

        encrypted_document = authorized_client
          .use('auto_encryption')['users']
          .find(_id: result.inserted_ids.first)
          .first

        expect(encrypted_document['ssn']).to be_ciphertext
      end
    end

    context 'after performing operation with auto encryption' do
      before do
        key_vault_collection.drop
        key_vault_collection.insert_one(data_key)

        client['users'].insert_one(ssn: ssn)
        client.close
      end

      it_behaves_like 'a functioning auto-encrypter'
    end

    context 'after performing operation without auto encryption' do
      before do
        client['users'].insert_one(age: 23)
        client.close
      end

      it_behaves_like 'a functioning auto-encrypter'
    end
  end
end
