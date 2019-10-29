require 'mongo'
require 'base64'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::Handle do
  require_libmongocrypt

  describe '#initialize' do
    let(:handle) { described_class.new(options) }
    let(:options) { { kms_providers: kms_providers } }

    context 'with empty options' do
      let(:options) { nil }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /Options must not be blank/)
      end
    end

    context 'with empty kms_providers' do
      let(:kms_providers) { {} }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /must have one of the following keys: :aws, :local/)
      end
    end

    context 'with aws kms_providers key' do
      let(:kms_providers) { { aws: {} } }

      it 'raises an exception' do
        expect { handle }.to raise_error(ArgumentError, /:aws is not yet a supported kms_providers option/)
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
        expect { handle }.to raise_error(Mongo::Error::CryptError, 'Client error with code 1: local key must be 96 bytes')
      end
    end

    context 'with valid local kms_providers' do
      after do
        handle.close
      end

      let(:kms_providers) do
        {
          local: {
            key: Base64.encode64('ruby' * 24)
          }
        }
      end

      it 'does not raise an exception' do
        expect { handle }.not_to raise_error
      end
    end
  end
end
