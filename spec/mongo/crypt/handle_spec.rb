require 'mongo'
require 'base64'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::Handle do
  require_libmongocrypt

  describe '#initialize' do
    let(:handle) { described_class.new(kms_providers, schema_map) }

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
        expect { handle }.to raise_error(Mongo::Error::CryptClientError, 'Code 1: local key must be 96 bytes')
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

    context 'with nil schema map' do
      let(:schema_map) { nil }

      it 'does not raise an exception' do
        expect { handle }.not_to raise_error
      end
    end
  end
end
