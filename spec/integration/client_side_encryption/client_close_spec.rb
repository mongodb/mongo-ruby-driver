require 'spec_helper'

describe 'Auto encryption client' do
  context 'after client is disconnected' do

    include_context 'define shared FLE helpers'
    include_context 'with local kms_providers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: kms_providers,
            key_vault_namespace: 'admin.datakeys',
            schema_map: { 'auto_encryption.users' => schema_map },
          },
          database: :auto_encryption,
        )
      )
    end

    context 'after performing operation with auto encryption' do
      before do
        client[:users].insert_one(ssn: ssn)
        client.close
      end

      it 'can still perform encryption' do
        result = client[:users].insert_one(ssn: '000-000-0000')
        expect(result).to be_ok

        encrypted_document = authorized_client
          .use(:auto_encryption)[:users]
          .find(_id: result.inserted_ids.first)
          .first

        expect(encrypted_document['ssn']).to be_ciphertext
      end
    end

    context 'after performing operation without auto encryption' do
      before do
        client[:users].insert_one(age: 23)
        client.close
      end

      it 'can still perform encryption' do
        result = client[:users].insert_one(ssn: '000-000-0000')
        expect(result).to be_ok

        encrypted_document = authorized_client
          .use(:auto_encryption)[:users]
          .find(_id: result.inserted_ids.first)
          .first

        expect(encrypted_document['ssn']).to be_ciphertext
      end
    end
  end
end
