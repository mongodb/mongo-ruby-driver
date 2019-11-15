require 'spec_helper'

describe 'Explicit Encryption' do
  require_libmongocrypt

  let(:client) { ClientRegistry.instance.new_local_client(['localhost:27017']) }
  let(:key_vault_namespace) { 'test.keys' }

  let(:options) do
    {
      kms_providers: {
        local: { key: Base64.encode64("ru\xfe\x00" * 24) }
      },
      key_vault_namespace: key_vault_namespace
    }
  end

  shared_examples_for 'an explicit encrypter' do
    it 'encrypts and decrypts the value' do
      client_encryption = Mongo::ClientEncryption.new(
        client,
        options
      )

      data_key_id = client_encryption.create_data_key

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

      client_encryption.close
      client.close
    end
  end

  context 'value is a string' do
    let(:value) { 'Hello, world!' }

    it_behaves_like 'an explicit encrypter'
  end

  context 'value is an integer' do
    let(:value) { 42 }

    it_behaves_like 'an explicit encrypter'
  end

  context 'using block API' do
    let(:value) { 'Hello, world!' }

    it 'performs encryption and decryption' do
      encrypted = Mongo::ClientEncryption.with_client_encryption(client, options) do |client_encryption|
        data_key_id = client_encryption.create_data_key
        encrypted = client_encryption.encrypt(
          value,
          {
            key_id: data_key_id,
            algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
          }
        )
      end

      decrypted = Mongo::ClientEncryption.with_client_encryption(client, options) do |client_encryption|
        client_encryption.decrypt(encrypted)
      end

      expect(decrypted).to eq(value)
      expect(decrypted).to be_a_kind_of(String)

      client.close
    end
  end
end
