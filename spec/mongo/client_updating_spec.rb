require 'spec_helper'

describe Mongo::Client do
  clean_slate

  context 'auto encryption options' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'
    include_context 'with local kms_providers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: kms_providers,
            key_vault_namespace: key_vault_namespace,
            schema_map: { 'auto_encryption.users' => schema_map },
          }
        )
      )
    end

    let(:auto_encryption_options) do
      {
        key_vault_namespace: key_vault_namespace,
        kms_providers: kms_providers,
        schema_map: schema_map
      }
    end

    let(:new_auto_encryption_options) do
      {
        key_vault_namespace: 'new.namespace',
        kms_providers: kms_providers
      }
    end

    describe '#with' do
      it 'updates auto encryption options' do
        new_client = client.with(auto_encryption_options: new_auto_encryption_options)
        expect(new_client.encryption_options[:key_vault_namespace]).to eq('new.namespace')
      end

      it 'removes auto encryption options' do
        new_options = { auto_encryption_options: nil }
        new_client = client.with(new_options)

        expect(new_client.encryption_options).to be_nil
      end
    end

    describe '#use' do
      before do
        authorized_client.use(:admin)[:datakeys].drop
        authorized_client.use(:admin)[:datakeys].insert_one(data_key)
      end

      it 'can still perform auto encryption' do
        new_client = client.use(:auto_encryption)
        new_client[:users].insert_one(ssn: ssn)

        doc = authorized_client.use(:auto_encryption)[:users].find.first
        expect(doc['ssn']).to be_ciphertext
      end
    end
  end
end
