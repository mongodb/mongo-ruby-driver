require 'lite_spec_helper'
require 'mongo'

require 'base64'

describe Mongo::ClientEncryption do
  require_libmongocrypt

  let(:key_vault_db) { 'admin' }
  let(:key_vault_coll) { 'datakeys' }
  let(:key_vault_namespace) { "#{key_vault_db}.#{key_vault_coll}" }

  let(:client) do
    ClientRegistry.instance.new_local_client(
      [SpecConfig.instance.addresses.first]
    )
  end

  let(:kms_providers) do
    {
      local: {
        key: "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"
      }
    }
  end

  let(:client_encryption) do
    described_class.new(client, {
      key_vault_namespace: key_vault_namespace,
      kms_providers: kms_providers
    })
  end

  describe '#initialize' do
    let(:client) { new_local_client_nmio([SpecConfig.instance.addresses.first]) }

    context 'with nil key_vault_namespace' do
      let(:key_vault_namespace) { nil }

      it 'raises an exception' do
        expect do
          client_encryption
        end.to raise_error(ArgumentError, /:key_vault_namespace option cannot be nil/)
      end
    end

    context 'with invalid key_vault_namespace' do
      let(:key_vault_namespace) { 'three.word.namespace' }

      it 'raises an exception' do
        expect do
          client_encryption
        end.to raise_error(ArgumentError, /invalid key vault namespace/)
      end
    end

    context 'with invalid KMS provider information' do
      let(:kms_providers) { { random_key: {} } }

      it 'raises an exception' do
        expect do
          client_encryption
        end.to raise_error(ArgumentError, /kms_providers option must have one of the following keys/)
      end
    end

    context 'with valid options' do
      it 'creates a ClientEncryption object' do
        expect do
          client_encryption
        end.not_to raise_error
      end
    end
  end

  describe '#create_data_key' do
    let(:result) { client_encryption.create_data_key }

    it 'returns a string' do
      expect(result).to be_a_kind_of(String)

      # make sure that the key actually exists in the DB
      expect(client.use(key_vault_db)[key_vault_coll].find(_id: BSON::Binary.new(result, :uuid)).count).to eq(1)
    end
  end

  shared_context 'encryption/decryption' do
    let(:data_key) do
      Utils.parse_extended_json(JSON.parse(File.read('spec/mongo/crypt/data/key_document.json')))
    end

    # Represented in as Base64 for simplicity
    let(:encrypted_value) { "bwAAAAV2AGIAAAAGASzggCwAAAAAAAAAAAAAAAACk0TG2WPKVdChK2Oay9QT\nYNYHvplIMWjXWlnxAVC2hUwayNZmKBSAVgW0D9tnEMdDdxJn+OxqQq3b9MGI\nJ4pHUwVPSiNqfFTKu3OewGtKV9AA\n" }
    let(:value) { 'Hello world' }

    before do
      key_vault_collection = client.use(key_vault_db)[key_vault_coll]
      key_vault_collection.drop

      key_vault_collection.insert_one(data_key)
    end
  end

  describe '#encrypt' do
    include_context 'encryption/decryption'

    it 'returns the correct encrypted string' do
      encrypted = client_encryption.encrypt(
        value,
        {
          key_id: data_key['_id'].data,
          algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
        }
      )

      expect(encrypted).to be_a_kind_of(String)
      expect(encrypted).to eq(Base64.decode64(encrypted_value))
    end
  end

  describe '#decrypt' do
    include_context 'encryption/decryption'

    it 'returns the correct unencrypted value' do
      result = client_encryption.decrypt(Base64.decode64(encrypted_value))
      expect(result).to eq(value)
    end
  end
end
