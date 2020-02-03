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
  let(:options) { {} }

  describe '#initialize' do
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

      it 'does not raise an exception' do
        expect do
          context
        end.not_to raise_error
      end
    end

    context 'with aws kms provider' do
      include_context 'with AWS kms_providers'

      context 'with empty options' do
        let(:options) { {} }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /master key options cannot be nil/)
        end
      end

      context 'with an invalid master key option' do
        let(:options) { { master_key: 'key' } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /key is an invalid master key option/)
        end
      end

      context 'where master key is an empty hash' do
        let(:options) { { master_key: {} } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The :region key of the :master_key options Hash cannot be nil/)
        end
      end

      context 'with a nil region option' do
        let(:options) { { master_key: { region: nil } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The :region key of the :master_key options Hash cannot be nil/)
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
          end.to raise_error(ArgumentError, /The :key key of the :master_key options Hash cannot be nil/)
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
        let(:options) { { master_key: { region: 'us-east-2', key: 'arn' } } }

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
