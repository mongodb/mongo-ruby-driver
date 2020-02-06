require 'spec_helper'

describe 'Explicit Encryption' do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:client) { ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses) }

  let(:client_encryption_opts) do
    {
      kms_providers: kms_providers,
      key_vault_namespace: key_vault_namespace
    }
  end

  shared_examples 'an explicit encrypter' do
    it 'encrypts and decrypts the value' do
      client_encryption = Mongo::ClientEncryption.new(
        client,
        client_encryption_opts
      )

      data_key_id = client_encryption.create_data_key(
        kms_provider_name,
        data_key_options
      )

      encrypted = client_encryption.encrypt(
        value,
        {
          key_id: data_key_id,
          algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
        }
      )

      decrypted = client_encryption.decrypt(encrypted)
      expect(decrypted).to eq(value)
      expect(decrypted).to be_a_kind_of(value.class)
    end
  end

  context 'value is a string' do
    let(:value) { 'Hello, world!' }

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      it_behaves_like 'an explicit encrypter'
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      it_behaves_like 'an explicit encrypter'
    end
  end

  context 'value is an integer' do
    let(:value) { 42 }

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      it_behaves_like 'an explicit encrypter'
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      it_behaves_like 'an explicit encrypter'
    end
  end

  context 'value is an symbol' do
    let(:value) { BSON::Symbol::Raw.new(:hello_world) }

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      it_behaves_like 'an explicit encrypter'
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      it_behaves_like 'an explicit encrypter'
    end
  end
end
