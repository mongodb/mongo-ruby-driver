# frozen_string_literal: true

require 'spec_helper'

describe 'On-demand Azure Credentials' do
  require_libmongocrypt
  include_context 'define shared FLE helpers'
  include_context 'with Azure kms_providers'

  let(:client) { ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses) }

  let(:client_encryption_opts) do
    {
      kms_providers: { azure: {} },
      kms_tls_options: kms_tls_options,
      key_vault_namespace: key_vault_namespace
    }
  end

  let(:client_encryption) do
    Mongo::ClientEncryption.new(
      client,
      client_encryption_opts
    )
  end

  context 'when credentials are available' do
    it 'authenticates successfully' do
      skip 'This tests should be run inside Azure Cloud only' unless ENV['TEST_FLE_AZURE_AUTO']

      expect do
        client_encryption.create_data_key('azure', data_key_options)
      end.not_to raise_error
    end
  end

  context 'when credentials are not available' do
    it 'raises an error' do
      skip 'This tests should NOT be run inside Azure Cloud only' if ENV['TEST_FLE_AZURE_AUTO']

      expect do
        client_encryption.create_data_key('azure', data_key_options)
      end.to raise_error(Mongo::Error::CryptError, /Azure credentials/)
    end
  end
end
