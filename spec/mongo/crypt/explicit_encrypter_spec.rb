require 'mongo'
require 'spec_helper'

describe Mongo::Crypt::ExplicitEncrypter do
  require_libmongocrypt
  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:explicit_encrypter) do
    described_class.new(options)
  end

  let(:key_vault_client) { authorized_client }

  let(:options) do
    {
      kms_providers: kms_providers,
      key_vault_namespace: key_vault_namespace,
      key_vault_client: key_vault_client
    }
  end

  describe '#initialize' do
    context 'without key_vault_namespace option' do
      let(:options) do
        {
          key_vault_client: key_vault_client,
          kms_providers: kms_providers
        }
      end

      it 'raises an exception' do
        expect do
          explicit_encrypter
        end.to raise_error(ArgumentError, /The :key_vault_namespace option cannot be nil/)
      end
    end

    context 'with invalid key_vault_namespace' do
      let(:options) do
        {
          key_vault_client: key_vault_client,
          kms_providers: kms_providers,
          key_vault_namespace: 'key.vault.namespace'
        }
      end

      it 'raises an exception' do
        expect do
          explicit_encrypter
        end.to raise_error(ArgumentError, /key.vault.namespace is an invalid key vault namespace.The :key_vault_namespace option must be in the format database.collection/)
      end
    end

    context 'without key_vault_client option' do
      let(:options) do
        {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace
        }
      end

      it 'raises an exception' do
        expect do
          explicit_encrypter
        end.to raise_error(ArgumentError, /The :key_vault_client option cannot be nil/)
      end
    end

    context 'with invalid key_vault_client' do
      let(:options) do
        {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          key_vault_client: 'A string'
        }
      end

      it 'raises an exception' do
        expect do
          explicit_encrypter
        end.to raise_error(ArgumentError, /The :key_vault_client option must be an instance of Mongo::Client/)
      end
    end

    context 'without kms_providers option' do
      let(:options) do
        {
          key_vault_namespace: key_vault_namespace,
          key_vault_client: key_vault_client
        }
      end

      it 'raises an exception' do
        expect do
          explicit_encrypter
        end.to raise_error(ArgumentError, /The kms_providers option must not be nil/)
      end
    end

    context 'with valid options' do
      it 'initializes the ExplicitEncrypter' do
        expect do
          explicit_encrypter
        end.not_to raise_error
      end
    end
  end

  describe '#create_and_insert_data_key' do
    let(:result) do
      explicit_encrypter.create_and_insert_data_key(kms_provider, data_key_options)
    end
  end

  describe '#encrypt' do

  end

  describe '#decrypt' do

  end
end
