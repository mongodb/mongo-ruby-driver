require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::DataKeyContext do
  require_libmongocrypt

  let(:mongocrypt) { Mongo::Crypt::Binding.mongocrypt_new }
  let(:context) { described_class.new(mongocrypt) }

  let(:master_key) do
    bytes = ("ru\xfe\x00" * 24).unpack('C*')

    p = FFI::MemoryPointer
    .new(bytes.size)
    .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, bytes)

    Mongo::Crypt::Binding.mongocrypt_binary_new_from_data(p, bytes.length)
  end

  after do
    Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
  end

  describe '#initialize' do
    context 'when no kms provider has been set on mongocrypt' do
      before do
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      it 'raises an exception' do
        expect do
          context
        end.to raise_error(Mongo::Error::CryptClientError, /requested kms provider not configured/)
      end
    end

    context 'when local kms provider has been set on mongocrypt' do
      before do
        Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, master_key)
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      after do
        Mongo::Crypt::Binding.mongocrypt_binary_destroy(master_key)
      end

      it 'does not raise an exception' do
        expect do
          context
        end.not_to raise_error
      end
    end
  end

  # This is a simple spec just to test that this method works
  # There should be multiple specs testing the context's state
  #   depending on how it's initialized, etc.
  describe '#state' do
    before do
      Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, master_key)
      Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
    end

    after do
      Mongo::Crypt::Binding.mongocrypt_binary_destroy(master_key)
    end

    it 'returns :ready' do
      expect(context.state).to eq(:ready)
    end
  end

  # This is a simple spec just to test the POC case of creating a data key
  # There should be specs testing each state, as well as integration tests
  #   to test that the state machine returns the correct result under various
  #   conditions
  describe '#run_state_machine' do
    before do
      Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, master_key)
      Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
    end

    after do
      Mongo::Crypt::Binding.mongocrypt_binary_destroy(master_key)
    end

    it 'creates a data key' do
      expect(context.run_state_machine).to be_a_kind_of(String)
    end
  end
end
