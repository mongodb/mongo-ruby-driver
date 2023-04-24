# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'On-demand AWS Credentials' do
  require_libmongocrypt
  include_context 'define shared FLE helpers'
  include_context 'with AWS kms_providers'

  let(:client) { ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses) }

  let(:client_encryption_opts) do
    {
      kms_providers: { aws: {} },
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
      expect do
        client_encryption.create_data_key('aws', data_key_options)
      end.not_to raise_error
    end
  end

  context 'when credentials are not available' do
    it 'raises an error' do
      expect_any_instance_of(
        Mongo::Auth::Aws::CredentialsRetriever
      ).to receive(:credentials).with(no_args).once.and_raise(
        Mongo::Auth::Aws::CredentialsNotFound
      )

      expect do
        client_encryption.create_data_key('aws', data_key_options)
      end.to raise_error(Mongo::Error::CryptError, /Could not locate AWS credentials/)
    end
  end
end
