require 'mongo'
require 'base64'
require 'lite_spec_helper'

describe Mongo::Crypt::Handle do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  describe '#initialize' do
    let(:handle) { described_class.new(kms_providers, schema_map: schema_map) }
    let(:schema_map) { nil }

    shared_examples 'a functioning Mongo::Crypt::Handle' do
      context 'with valid schema map' do
        it 'does not raise an exception' do
          expect { handle }.not_to raise_error
        end
      end

      context 'with invalid schema map' do
        let(:schema_map) { '' }

        it 'raises an exception' do
          expect { handle }.to raise_error(ArgumentError, /schema_map must be a Hash or nil/)
        end
      end

      context 'with nil schema map' do
        let(:schema_map) { nil }

        it 'does not raise an exception' do
          expect { handle }.not_to raise_error
        end
      end
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
            key: 'ruby' * 23 # NOT 96 bytes
          }
        }
      end

      it 'raises an exception' do
        expect { handle }.to raise_error(Mongo::Error::CryptError, 'local key must be 96 bytes (libmongocrypt error code 1)')
      end
    end

    context 'with valid local kms_providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning Mongo::Crypt::Handle'
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
            secret_access_key: SpecConfig.instance.fle_aws_secret
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The aws access_key_id option must be a String with at least one character; currently have nil/)
      end
    end

    context 'with non-string AWS access_key_id' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: 5,
            secret_access_key: SpecConfig.instance.fle_aws_secret
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The aws access_key_id option must be a String with at least one character; currently have 5/)
      end
    end

    context 'with empty string AWS access_key_id' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: '',
            secret_access_key: SpecConfig.instance.fle_aws_secret
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The aws access_key_id option must be a String with at least one character; it is currently an empty string/)
      end
    end


    context 'with nil AWS secret_access_key' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: nil
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The aws secret_access_key option must be a String with at least one character; currently have nil/)
      end
    end

    context 'with non-string AWS secret_access_key' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: 5
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The aws secret_access_key option must be a String with at least one character; currently have 5/)
      end
    end

    context 'with empty string AWS secret_access_key' do
      let(:kms_providers) {
        {
          aws: {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: ''
          }
        }
      }

      it 'raises an exception' do
        expect do
          handle
        end.to raise_error(ArgumentError, /The aws secret_access_key option must be a String with at least one character; it is currently an empty string/)
      end
    end

    context 'with valid AWS kms_providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning Mongo::Crypt::Handle'
    end
  end
end
