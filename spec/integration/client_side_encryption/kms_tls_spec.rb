# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: KMS TLS Tests' do
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
          kms_tls_options: {
            aws: default_kms_tls_options_for_provider
          },
          key_vault_namespace: 'keyvault.datakeys',
        },
      )
    end

    context 'invalid KMS certificate' do
      it 'raises an error when creating data key' do
        expect do
          client_encryption.create_data_key(
            'aws',
            {
              master_key: {
                region: "us-east-1",
                key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                endpoint: "127.0.0.1:8000",
              }
           }
          )
        end.to raise_error(Mongo::Error::KmsError, /certificate verify failed/)
      end
    end

    context 'Invalid Hostname in KMS Certificate' do
      context 'MRI' do
        require_mri

        it 'raises an error when creating data key' do
          expect do
            client_encryption.create_data_key(
              'aws',
              {
                master_key: {
                  region: "us-east-1",
                  key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                  endpoint: "127.0.0.1:8001",
                }
            }
            )
          end.to raise_error(Mongo::Error::KmsError, /certificate verify failed/)
        end
      end

      context 'JRuby' do
        require_jruby

        it 'raises an error when creating data key' do
          expect do
            client_encryption.create_data_key(
              'aws',
              {
                master_key: {
                  region: "us-east-1",
                  key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                  endpoint: "127.0.0.1:8001",
                }
            }
            )
          end.to raise_error(Mongo::Error::KmsError, /hostname mismatch/)
        end
      end
    end

  end
end
