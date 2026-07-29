# frozen_string_literal: true

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: Custom Key Material Test' do
    require_libmongocrypt
    include_context 'define shared FLE helpers'

    let(:client) do
      ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses)
    end

    let(:client_encryption) do
      Mongo::ClientEncryption.new(
        client,
        key_vault_namespace: key_vault_namespace,
        kms_providers: local_kms_providers
      )
    end

    let(:key_vault_collection) do
      client.use(key_vault_db)[key_vault_coll, write_concern: { w: :majority }]
    end

    # 96 bytes of custom key material, given by the spec as base64.
    let(:key_material) do
      BSON::Binary.new(
        Base64.decode64(
          'xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC' \
          '+HVjSIgdL+RKwZCvpXSyxTICWSXTUYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5'
        )
      )
    end

    # The all-zero UUID the key document is re-inserted under.
    let(:key_id) do
      BSON::Binary.new(Base64.decode64('AAAAAAAAAAAAAAAAAAAAAA=='), :uuid)
    end

    let(:expected_ciphertext) do
      'AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/t9wLifJnnNYwiw79d' \
        '75QYIZ6M/aYC1h9nCzCjZ7pGUpAuNnkUhnIXM3PjrA=='
    end

    before do
      key_vault_collection.drop
    end

    it 'encrypts with the custom key material' do
      created_key_id = client_encryption.create_data_key(
        'local',
        key_material: key_material
      )

      key_document = key_vault_collection.find(_id: created_key_id).first
      key_vault_collection.delete_one(_id: created_key_id)

      key_vault_collection.insert_one(key_document.merge('_id' => key_id))

      encrypted = client_encryption.encrypt(
        'test',
        key_id: key_id,
        algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
      )

      expect(Base64.strict_encode64(encrypted.data)).to eq(expected_ciphertext)
    end
  end
end
