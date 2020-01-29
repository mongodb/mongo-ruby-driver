require 'mongo'
require 'base64'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::Handle do
  require_libmongocrypt

  describe '#initialize' do
    let(:handle) { described_class.new(kms_providers, schema_map: schema_map) }

    let(:kms_providers) do
      {
        local: {
          key: Base64.encode64("ru\xfe\x00" * 24)
        }
      }
    end

    let(:schema_map) do
      {
        'admin.datakeys': {
          bsonType: 'object',
          properties: {
            ssn: {
              encrypt: {
                keyId: BSON::Binary.new("e114f7ad-ad7a-4a68-81a7-ebcb9ea0953a", :uuid),
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
              }
            }
          }
        }
      }
    end

    # TODO: replace with environment variables
    let(:aws_key) { 'temp-aws-key' }
    let(:aws_secret) { 'temp-aws-secret' }

    context 'with empty kms_providers' do
      let(:kms_providers) { {} }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /must have one of the following keys: :aws, :local/)
      end
    end

    context 'with invalid aws kms_providers' do
      let(:kms_providers) { { aws: {} } }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /kms_providers with :aws key must be in the format: { aws: { access_key_id: 'YOUR-ACCESS-KEY-ID', secret_access_key: 'SECRET-ACCESS-KEY' } }/)
      end
    end

    context 'with invalid kms_providers key' do
      let(:kms_providers) { { random_kms_provider: {} } }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /must have one of the following keys: :aws, :local/)
      end
    end

    context 'with empty local kms_providers' do
      let(:kms_providers) { { local: {} } }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /kms_providers with :local key must be in the format: { local: { key: 'MASTER-KEY' } }/)
      end
    end

    context 'with invalid local kms_providers' do
      let(:kms_providers) { { local: { invalid_key: 'Some stuff' } } }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /kms_providers with :local key must be in the format: { local: { key: 'MASTER-KEY' } }/)
      end
    end

    context 'with invalid local kms master key' do
      let(:kms_providers) do
        {
          local: {
            key: Base64.encode64('ruby' * 23) # NOT 96 bytes
          }
        }
      end

      it 'raises an exception' do
        expect { handle }.to raise_error(Mongo::Error::CryptError, 'local key must be 96 bytes')
      end
    end

    context 'with invalid schema map' do
      let(:schema_map) { '' }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /schema_map must be a Hash or nil/)
      end
    end

    context 'with valid local kms_providers and schema map' do
      let(:kms_providers) do
        {
          local: {
            key: Base64.encode64("ru\xfe\x00" * 24)
          }
        }
      end

      it 'does not raise an exception' do
        expect { handle }.not_to raise_error
      end
    end

    context 'with nil AWS kms_provider' do
      let(:kms_providers) {
        {
          aws: nil
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The :aws KMS provider must not be nil/)
      end
    end

    context 'with empty AWS kms_provider' do
      let(:kms_providers) {
        {
          aws: {}
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The specified aws kms_providers option is invalid/)
      end
    end

    context 'with nil AWS access_key_id' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: nil,
            secret_access_key: aws_secret
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The specified aws kms_providers option is invalid/)
      end
    end

    context 'with non-string AWS access_key_id' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: 5,
            secret_access_key: aws_secret
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The specified aws kms_providers option is invalid/)
      end
    end


    context 'with nil AWS secret_access_key' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: aws_key,
            secret_access_key: nil
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The specified aws kms_providers option is invalid/)
      end
    end

    context 'with non-string AWS secret_access_key' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: aws_key,
            secret_access_key: 5
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The specified aws kms_providers option is invalid/)
      end
    end

    context 'with valid AWS kms_providers and schema map' do
      let(:kms_providers) do
        {
          aws: {
            # TODO: replace with environment variables
            access_key_id: aws_key,
            secret_access_key: aws_secret
          }
        }
      end

      it 'does not raise an exception' do
        expect { handle }.not_to raise_error
      end
    end

    context 'with nil schema map' do
      let(:schema_map) { nil }

      it 'does not raise an exception' do
        expect { handle }.not_to raise_error
      end
    end
  end
end
