# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'On-demand GCP Credentials' do
  require_libmongocrypt
  include_context 'define shared FLE helpers'
  include_context 'with GCP kms_providers'

  let(:client) { ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses) }

  let(:client_encryption_opts) do
    {
      kms_providers: { gcp: {} },
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
      skip 'This tests should be run inside Google Cloud only' unless ENV['TEST_FLE_GCP_AUTO']
      expect do
        client_encryption.create_data_key('gcp', data_key_options)
      end.not_to raise_error
    end
  end

  context 'when credentials are not available' do
    it 'raises an error' do
      expect(
        Mongo::Crypt::KMS::GCP::CredentialsRetriever
      ).to receive(:get_access_token).with(no_args).once.and_raise(
        Mongo::Crypt::KMS::CredentialsNotFound
      )

      expect do
        client_encryption.create_data_key('gcp', data_key_options)
      end.to raise_error(Mongo::Error::CryptError, /GCP credentials/)
    end
  end
end

