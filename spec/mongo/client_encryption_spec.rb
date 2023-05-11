# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::ClientEncryption do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options
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
        end.to raise_error(ArgumentError, /KMS providers options must have one of the following keys/)
      end
    end
  end

  describe '#create_data_key' do
    let(:data_key_id) { client_encryption.create_data_key(kms_provider_name, options) }
    let(:key_alt_names) { nil }

    shared_examples 'it creates a data key' do |with_key_alt_names: false|
      it 'returns the data key id and inserts it into the key vault collection' do
        expect(data_key_id).to be_uuid

        documents = client.use(key_vault_db)[key_vault_coll].find(_id: data_key_id)

        expect(documents.count).to eq(1)

        if with_key_alt_names
          expect(documents.first['keyAltNames']).to match_array(key_alt_names)
        else
          expect(documents.first['keyAltNames']).to be_nil
        end
      end
    end

    shared_examples 'it supports key_alt_names' do
      let(:options) { base_options.merge(key_alt_names: key_alt_names) }

      context 'with one value in key_alt_names' do
        let(:key_alt_names) { ['keyAltName1'] }
        it_behaves_like 'it creates a data key', **{ with_key_alt_names: true }
      end

      context 'with multiple values in key_alt_names' do
        let(:key_alt_names) { ['keyAltName1', 'keyAltName2'] }
        it_behaves_like 'it creates a data key', **{ with_key_alt_names: true }
      end

      context 'with empty key_alt_names' do
        let(:key_alt_names) { [] }
        it_behaves_like 'it creates a data key'
      end

      context 'with invalid key_alt_names option' do
        let(:key_alt_names) { 'keyAltName1' }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /key_alt_names option must be an Array/)
        end
      end

      context 'with invalid key_alt_names values' do
        let(:key_alt_names) { ['keyAltNames1', 3] }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /values of the :key_alt_names option Array must be Strings/)
        end
      end
    end

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      let(:base_options) { { master_key: { region: aws_region, key: aws_arn } } }
      it_behaves_like 'it supports key_alt_names'

      context 'with nil options' do
        let(:options) { nil }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /Key document options must not be nil/)
        end
      end

      context 'with nil master key' do
        let(:options) { { master_key: nil } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /Key document options must contain a key named :master_key with a Hash value/)
        end
      end

      context 'with invalid master key' do
        let(:options) { { master_key: 'master-key' } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /Key document options must contain a key named :master_key with a Hash value/)
        end
      end

      context 'with empty master key' do
        let(:options) { { master_key: {} } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /The specified KMS provider options are invalid: {}. AWS key document  must be in the format: { region: 'REGION', key: 'KEY' }/)
        end
      end

      context 'with nil region' do
        let(:options) { { master_key: { region: nil, key: aws_arn } } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /The region option must be a String with at least one character; currently have nil/)
        end
      end

      context 'with invalid region' do
        let(:options) { { master_key: { region: 5, key: aws_arn } } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /The region option must be a String with at least one character; currently have 5/)
        end
      end

      context 'with nil key' do
        let(:options) { { master_key: { key: nil, region: aws_region } } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /The key option must be a String with at least one character; currently have nil/)
        end
      end

      context 'with invalid key' do
        let(:options) { { master_key: { key: 5, region: aws_region } } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /The key option must be a String with at least one character; currently have 5/)
        end
      end

      context 'with invalid endpoint' do
        let(:options) { { master_key: { key: aws_arn, region: aws_region, endpoint: 5 } } }

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(ArgumentError, /The endpoint option must be a String with at least one character; currently have 5/)
        end
      end

      context 'with nil endpoint' do
        let(:options) do
          {
            master_key: {
              key: aws_arn,
              region: aws_region,
              endpoint: nil
            }
          }
        end

        it_behaves_like 'it creates a data key'
      end

      context 'with valid endpoint, no port' do
        let(:options) do
          {
            master_key: {
              key: aws_arn,
              region: aws_region,
              endpoint: aws_endpoint_host
            }
          }
        end

        it_behaves_like 'it creates a data key'
      end

      context 'with valid endpoint' do
        let(:options) { data_key_options }
        it_behaves_like 'it creates a data key'
      end

      context 'with https' do
        let(:options) do
          {
            master_key: {
              key: aws_arn,
              region: aws_region,
              endpoint: "https://#{aws_endpoint_host}:#{aws_endpoint_port}"
            }
          }
        end

        it_behaves_like 'it creates a data key'
      end

      context 'with invalid endpoint' do
        let(:options) do
          {
            master_key: {
              key: aws_arn,
              region: aws_region,
              endpoint: "invalid-nonsense-endpoint.com"
            }
          }
        end

        it 'raises an exception' do
          expect do
            data_key_id
          end.to raise_error(Mongo::Error::KmsError, /SocketError/)
        end
      end

      context 'when socket connect errors out' do
        let(:options) { data_key_options }

        before do
          allow_any_instance_of(OpenSSL::SSL::SSLSocket)
            .to receive(:connect)
            .and_raise('Error while connecting to socket')
        end

        it 'raises a KmsError' do
          expect do
            data_key_id
          end.to raise_error(Mongo::Error::KmsError, /Error while connecting to socket/)
        end
      end

      context 'when socket connect errors out' do
        let(:options) { data_key_options }

        before do
          allow_any_instance_of(OpenSSL::SSL::SSLSocket)
            .to receive(:sysclose)
            .and_raise('Error while closing socket')
        end

        it 'does not raise an exception' do
          expect do
            data_key_id
          end.not_to raise_error
        end
      end
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'
      let(:options) { {} }
      let(:base_options) { {} }

      it_behaves_like 'it supports key_alt_names'
      it_behaves_like 'it creates a data key'
    end
  end

  describe '#encrypt/decrypt' do
    let(:value) { ssn }
    let(:encrypted_value) { encrypted_ssn }

    before do
      key_vault_collection.drop
      key_vault_collection.insert_one(data_key)
    end

    shared_examples 'an encrypter' do
      let(:encrypted) do
        client_encryption.encrypt(
          value,
          {
            key_id: key_id,
            key_alt_name: key_alt_name,
            algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
          }
        )
      end

      context 'with key_id option' do
        let(:key_alt_name) { nil }

        it 'correctly encrypts a string' do
          expect(encrypted).to be_ciphertext
          expect(encrypted.data).to eq(Base64.decode64(encrypted_value))
        end
      end

      context 'with key_alt_name option' do
        let(:key_id) { nil }

        it 'correctly encrypts a string' do
          expect(encrypted).to be_ciphertext
          expect(encrypted.data).to eq(Base64.decode64(encrypted_value))
        end
      end
    end

    shared_examples 'a decrypter' do
      it 'correctly decrypts a string' do
        encrypted = BSON::Binary.new(Base64.decode64(encrypted_value), :ciphertext)

        result = client_encryption.decrypt(encrypted)
        expect(result).to eq(value)
      end
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'

      it_behaves_like 'an encrypter'
      it_behaves_like 'a decrypter'
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'

      it_behaves_like 'an encrypter'
      it_behaves_like 'a decrypter'
    end
  end
end
