# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: Data key and double encryption' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options
      )
    end

    let(:client_encryption) do
      Mongo::ClientEncryption.new(
        client,
        {
          kms_providers: aws_kms_providers,
          key_vault_namespace: 'keyvault.datakeys',
        },
      )
    end

    let(:master_key_template) do
      {
        region: "us-east-1",
        key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
      }
    end

    let(:data_key_id) do
      client_encryption.create_data_key('aws', master_key: master_key)
    end

    shared_examples 'a functioning data key' do
      it 'can encrypt and decrypt a string' do
        encrypted = client_encryption.encrypt(
          'test',
          {
            key_id: data_key_id,
            algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
          }
        )

        expect(encrypted).to be_ciphertext

        decrypted = client_encryption.decrypt(encrypted)
        expect(decrypted).to eq('test')
      end
    end

    context 'with region and key options' do
      let(:master_key) do
        master_key_template
      end

      it_behaves_like 'a functioning data key'
    end

    context 'with region, key, and endpoint options' do
      let(:master_key) do
        master_key_template.merge({endpoint: "kms.us-east-1.amazonaws.com"})
      end

      it_behaves_like 'a functioning data key'
    end

    context 'with region, key, and endpoint with valid port' do
      let(:master_key) do
        master_key_template.merge({endpoint: "kms.us-east-1.amazonaws.com:443"})
      end

      it_behaves_like 'a functioning data key'
    end

    shared_examples 'raising a KMS error' do
      it 'throws an exception' do
        expect do
          data_key_id
        end.to raise_error(Mongo::Error::KmsError, error_regex)
      end
    end

    context 'with region, key, and endpoint with invalid port' do
      let(:master_key) do
        master_key_template.merge({endpoint: "kms.us-east-1.amazonaws.com:12345"})
      end

      let(:error_regex) do
        if BSON::Environment.jruby?
          /SocketError/
        else
          /Connection refused/
        end
      end

      it_behaves_like 'raising a KMS error'
    end


    context 'with region, key, and endpoint with invalid region' do
      let(:master_key) do
        master_key_template.merge({endpoint: "kms.us-east-2.amazonaws.com"})
      end

      let(:error_regex) do
        //
      end

      it_behaves_like 'raising a KMS error'
    end

    context 'with region, key, and endpoint at incorrect domain' do
      let(:master_key) do
        master_key_template.merge({endpoint: "doesnotexist.invalid"})
      end

      let(:error_regex) do
        /SocketError: getaddrinfo:/
      end

      it_behaves_like 'raising a KMS error'
    end
  end
end
