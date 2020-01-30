require 'mongo'
require 'base64'
require 'lite_spec_helper'

describe Mongo::Crypt::DataKeyContext do
  require_libmongocrypt

  let(:mongocrypt) do
    Mongo::Crypt::Handle.new(
      {
        local: { key: Base64.encode64("ru\xfe\x00" * 24) },
        aws: {
          access_key_id: ENV['MONGO_RUBY_DRIVER_AWS_KEY'],
          secret_access_key: ENV['MONGO_RUBY_DRIVER_AWS_SECRET'],
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
