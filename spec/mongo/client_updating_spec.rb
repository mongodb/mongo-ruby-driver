require 'spec_helper'

describe Mongo::Client do
  clean_slate

  describe '#update_options' do
    context 'auto encryption options' do
      require_libmongocrypt

      let(:client) { new_local_client_nmio([SpecConfig.instance.addresses.first], client_opts) }
      let(:client_opts) { { auto_encryption_options: auto_encryption_options } }

      let(:auto_encryption_options) do
        {
          key_vault_client: key_vault_client,
          key_vault_namespace: key_vault_namespace,
          kms_providers: kms_providers
        }
      end

      let(:key_vault_client) { new_local_client_nmio('mongodb://127.0.0.1:27018') }
      let(:key_vault_namespace) { 'database.collection' }

      let(:kms_local) { { key: Base64.encode64('ruby' * 24) } }
      let(:kms_providers) { { local: kms_local } }

      let(:new_auto_encryption_options) do
        {
          key_vault_client: key_vault_client,
          key_vault_namespace: 'new.namespace',
          kms_providers: kms_providers
        }
      end

      it 'updates auto encryption options' do
        client.update_options({ auto_encryption_options: new_auto_encryption_options })
        expect(client.encryption_options[:key_vault_namespace]).to eq('new.namespace')
      end

      it 'removes auto encryption options' do
        new_options = { auto_encryption_options: nil }
        client.update_options(new_options)

        expect(client.encryption_options).to be_nil
      end
    end
  end
end
