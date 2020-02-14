require 'mongo'
require 'base64'
require 'lite_spec_helper'

describe Mongo::Crypt::DataKeyContext do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:mongocrypt) do
    Mongo::Crypt::Handle.new(kms_providers)
  end

  let(:io) { double("Mongo::Crypt::EncryptionIO") }

  let(:context) { described_class.new(mongocrypt, io, kms_provider_name, options) }
  let(:base_options) { {} }
  let(:options) { base_options }

  describe '#initialize' do
    shared_examples 'it properly sets key_alt_names' do
      context 'with one key_alt_names' do
        let(:options) { base_options.merge(key_alt_names: ['keyAltName1']) }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with multiple key_alt_names' do
        let(:options) { base_options.merge(key_alt_names: ['keyAltName1', 'keyAltName2']) }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with empty key_alt_names' do
        let(:options) { base_options.merge(key_alt_names: []) }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with invalid key_alt_names' do
        let(:options) { base_options.merge(key_alt_names: ['keyAltName1', 3]) }

        it 'does raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /All values of the :key_alt_names option Array must be Strings/)
        end
      end

      context 'with non-array key_alt_names' do
        let(:options) { base_options.merge(key_alt_names: "keyAltName1") }

        it 'does raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /key_alt_names option must be an Array/)
        end
      end
    end

    context 'with invalid kms provider'do
      let(:kms_providers) { local_kms_providers }
      let(:kms_provider_name) { 'invalid' }

      it 'raises an exception' do
        expect do
          context
        end.to raise_exception(/invalid is an invalid kms provider/)
      end
    end

    context 'with local kms provider and empty options' do
      include_context 'with local kms_providers'

      it_behaves_like 'it properly sets key_alt_names'

      it 'does not raise an exception' do
        expect do
          context
        end.not_to raise_error
      end
    end

    context 'with aws kms provider' do
      include_context 'with AWS kms_providers'

      let(:base_options) { { master_key: { region: 'us-east-2', key: 'arn' } } }

      it_behaves_like 'it properly sets key_alt_names'

      context 'with empty options' do
        let(:options) { {} }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /options Hash must contain a key named :master_key with a Hash value/)
        end
      end

      context 'with an invalid master key option' do
        let(:options) { { master_key: 'key' } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /:master_key option must be a Hash/)
        end
      end

      context 'where master key is an empty hash' do
        let(:options) { { master_key: {} } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The value of :region option of the :master_key options hash cannot be nil/)
        end
      end

      context 'with a nil region option' do
        let(:options) { { master_key: { region: nil } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The value of :region option of the :master_key options hash cannot be nil/)
        end
      end

      context 'with an invalid region option' do
        let(:options) { { master_key: { region: 5 } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /5 is an invalid AWS master_key region/)
        end
      end

      context 'with an invalid key option' do
        let(:options) { { master_key: { region: 'us-east-2', key: nil } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The value of :key option of the :master_key options hash cannot be nil/)
        end
      end

      context 'with an invalid key option' do
        let(:options) { { master_key: { region: 'us-east-2', key: 5 } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /5 is an invalid AWS master_key key/)
        end
      end

      context 'with an invalid endpoint option' do
        let(:options) { { master_key: { region: 'us-east-2', key: 'arn', endpoint: 5 } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /5 is an invalid AWS master_key endpoint/)
        end
      end

      context 'with valid options' do
        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with valid endpoint' do
        let(:options) { { master_key: { region: 'us-east-2', key: 'arn', endpoint: 'endpoint/to/kms' } } }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end
    end
  end

  describe '#run_state_machine' do
    # TODO: test with AWS KMS provider

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      it 'creates a data key' do
        expect(context.run_state_machine).to be_a_kind_of(Hash)
      end
    end
  end
end
