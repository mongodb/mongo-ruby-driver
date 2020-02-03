require 'mongo'
require 'base64'
require 'lite_spec_helper'

describe Mongo::ClientEncryption do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:client) do
    ClientRegistry.instance.new_local_client(
      [SpecConfig.instance.addresses.first]
    )
  end

  let(:client_encryption) do
    described_class.new(client, {
      key_vault_namespace: key_vault_namespace,
      kms_providers: kms_providers
    })
  end

  describe '#initialize' do
    shared_examples 'a functioning ClientEncryption' do
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

      context 'with valid options' do
        it 'creates a ClientEncryption object' do
          expect do
            client_encryption
          end.not_to raise_error
        end
      end
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning ClientEncryption'
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning ClientEncryption'
    end

    context 'with invalid KMS provider information' do
      let(:kms_providers) { { random_key: {} } }

      it 'raises an exception' do
        expect do
          client_encryption
        end.to raise_error(ArgumentError, /kms_providers option must have one of the following keys/)
      end
    end
  end

  describe '#create_data_key' do
    let(:data_key_id) { client_encryption.create_data_key(kms_provider_name, options) }

    shared_examples 'data key creation' do
      it 'returns the data key id and inserts it into the key vault collection' do
        expect(data_key_id).to be_a_kind_of(String)
        expect(data_key_id.bytesize).to eq(16)

        documents = client.use(key_vault_db)[key_vault_coll].find(
          _id: BSON::Binary.new(data_key_id, :uuid)
        )

        expect(documents.count).to eq(1)
      end
    end

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      # context 'with nil options' do
      #   let(:options) { nil }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /options cannot be nil/)
      #   end
      # end

      # context 'with empty options' do
      #   let(:options) { {} }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /options Hash must contain a key named :master_key/)
      #   end
      # end

      # context 'with nil master key' do
      #   let(:options) { { master_key: nil } }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /:master_key options cannot be nil/)
      #   end
      # end

      # context 'with invalid master key' do
      #   let(:options) { { master_key: 'master-key' } }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /master-key is an invalid :master_key option/)
      #   end
      # end

      # context 'with empty master key' do
      #   let(:options) { { master_key: {} } }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /region key of the :master_key options Hash cannot be nil/)
      #   end
      # end

      # context 'with nil region' do
      #   let(:options) { { master_key: { region: nil, key: 'arn' } } }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /region key of the :master_key options Hash cannot be nil/)
      #   end
      # end

      # context 'with invalid region' do
      #   let(:options) { { master_key: { region: 5, key: 'arn' } } }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /region key of the :master_key options Hash must be a String/)
      #   end
      # end

      # context 'with nil key' do
      #   let(:options) { { master_key: { key: nil, region: 'us-east-1' } } }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /key key of the :master_key options Hash cannot be nil/)
      #   end
      # end

      # context 'with invalid key' do
      #   let(:options) { { master_key: { key: 5, region: 'us-east-1' } } }

      #   it 'raises an exception' do
      #     expect do
      #       data_key_id
      #     end.to raise_error(ArgumentError, /key key of the :master_key options Hash must be a String/)
      #   end
      # end

      # context 'with invalid endpoint' do; end
      # context 'with nil endpoint' do; end
      # context 'with valid endpoint' do; end

      let(:options) do
        {
          master_key: {
            region: 'us-east-1',
            key: 'arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0'
          }
        }
      end

      include_examples 'data key creation'
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'
      let(:options) { {} }

      include_examples 'data key creation'
    end
  end

  shared_context 'encryption/decryption' do
    let(:data_key) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/data_keys/key_document.json'))
    end

    # Represented in as Base64 for simplicity
    # let(:encrypted_value) { "bwAAAAV2AGIAAAAGASzggCwAAAAAAAAAAAAAAAACk0TG2WPKVdChK2Oay9QT\nYNYHvplIMWjXWlnxAVC2hUwayNZmKBSAVgW0D9tnEMdDdxJn+OxqQq3b9MGI\nJ4pHUwVPSiNqfFTKu3OewGtKV9AA\n" }
    let(:encrypted_value) { "ASzggCwAAAAAAAAAAAAAAAACk0TG2WPKVdChK2Oay9QTYNYHvplIMWjXWlnx\nAVC2hUwayNZmKBSAVgW0D9tnEMdDdxJn+OxqQq3b9MGIJ4pHUwVPSiNqfFTK\nu3OewGtKV9A=\n" }
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

      expect(encrypted).to be_a_kind_of(BSON::Binary)
      expect(encrypted.type).to eq(:ciphertext)
      expect(encrypted.data).to eq(Base64.decode64(encrypted_value))
    end
  end

  describe '#decrypt' do
    include_context 'encryption/decryption'

    it 'returns the correct unencrypted value' do
      encrypted = BSON::Binary.new(Base64.decode64(encrypted_value), :ciphertext)

      result = client_encryption.decrypt(encrypted)
      expect(result).to eq(value)
    end
  end
end
