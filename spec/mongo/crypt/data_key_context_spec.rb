require 'mongo'
require 'support/lite_constraints'
require 'base64'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::DataKeyContext do
  require_libmongocrypt

  let(:mongocrypt) do
    Mongo::Crypt::Handle.new(
      {
        local: { key: Base64.encode64("ru\xfe\x00" * 24) },
        aws: {
          access_key_id: ENV['FLE_AWS_ACCESS_KEY'],
          secret_access_key: ENV['FLE_AWS_SECRET_ACCESS_KEY']
        }
      }
    )
  end

  let(:io) { double("Mongo::Crypt::EncryptionIO") }

  let(:context) { described_class.new(mongocrypt, io, kms_provider, options) }
  let(:kms_provider) { 'local' }
  let(:options) { {} }

  describe '#initialize' do
    context 'with invalid kms provider'do
      let(:kms_provider) { 'invalid' }

      it 'raises an exception' do
        expect do
          context
        end.to raise_exception(/invalid is an invalid kms provider/)
      end
    end

    context 'with local kms provider and empty options' do
      it 'does not raise an exception' do
        expect do
          context
        end.not_to raise_error
      end
    end

    context 'with aws kms provider' do
      let(:kms_provider) { 'aws' }

      context 'with empty options' do
        let(:options) { {} }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /masterkey options cannot be nil/)
        end
      end

      context 'with an invalid masterkey option' do
        let(:options) { { masterkey: 'key' } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /key is an invalid masterkey option/)
        end
      end

      context 'where masterkey is an empty hash' do
        let(:options) { { masterkey: {} } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The :region key of the :masterkey options Hash cannot be nil/)
        end
      end

      context 'with a nil region option' do
        let(:options) { { masterkey: { region: nil } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The :region key of the :masterkey options Hash cannot be nil/)
        end
      end

      context 'with an invalid region option' do
        let(:options) { { masterkey: { region: 5 } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /5 is an invalid AWS masterkey region/)
        end
      end

      context 'with an invalid key option' do
        let(:options) { { masterkey: { region: 'us-east-2', key: nil } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /The :key key of the :masterkey options Hash cannot be nil/)
        end
      end

      context 'with an invalid key option' do
        let(:options) { { masterkey: { region: 'us-east-2', key: 5 } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /5 is an invalid AWS masterkey key/)
        end
      end

      context 'with an invalid endpoint option' do
        let(:options) { { masterkey: { region: 'us-east-2', key: 'arn', endpoint: 5 } } }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /5 is an invalid AWS masterkey endpoint/)
        end
      end

      context 'with valid options' do
        let(:options) { { masterkey: { region: 'us-east-2', key: 'arn' } } }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with valid endpoint' do
        let(:options) { { masterkey: { region: 'us-east-2', key: 'arn', endpoint: 'endpoint/to/kms' } } }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end
    end
  end

  # This is a simple spec just to test that this method works
  # There should be multiple specs testing the context's state
  #   depending on how it's initialized, etc.
  describe '#state' do
    it 'returns :ready' do
      expect(context.state).to eq(:ready)
    end
  end

  # This is a simple spec just to test the POC case of creating a data key
  # There should be specs testing each state, as well as integration tests
  #   to test that the state machine returns the correct result under various
  #   conditions
  describe '#run_state_machine' do
    it 'creates a data key' do
      expect(context.run_state_machine).to be_a_kind_of(Hash)
    end
  end
end
