require 'spec_helper'

describe 'Explicit Encryption' do
  let(:client) { ClientRegistry.instance.new_local_client(['localhost:27017']) }
  let(:key_vault_namespace) { 'test.keys' }

  let(:client_encryption_opts) do
    {
      kms_providers: {
        local: { key: Base64.encode64("ru\xfe\x00" * 24) }
      },
      key_vault_namespace: key_vault_namespace
    }
  end

  it 'encrypts a value' do
    client_encryption = Mongo::ClientEncryption.new(
      client,
      client_encryption_opts
    )

    data_key_id = client_encryption.create_data_key

    encrypted = client_encryption.encrypt(
      'Hello, world!',
      {
        key_id: data_key_id,
        algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
      }
    )

    expect(encrypted).to be_a_kind_of(BSON::Binary)
  end
end
