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
        expect_any_instance_of(described_class).to receive(:close).once

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
        context.close
      end

      it 'does not raise an exception' do
        expect do
          context
        end.not_to raise_error
      end
    end
  end

  describe '#with_context' do
    before do
      Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, master_key)
      Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)

      allow(described_class)
        .to receive(:new)
        .with(mongocrypt)
        .and_return(context)
    end

    after do
      Mongo::Crypt::Binding.mongocrypt_binary_destroy(master_key)
    end

    context 'when yield errors' do
      it 'closes the created context and raises the error' do
        expect(context).to receive(:close).once

        expect do
          described_class.with_context(mongocrypt) do |_|
            raise StandardError.new("an error")
          end
        end.to raise_error(StandardError, /an error/)
      end
    end

    it 'creates a new context and closes it' do
      expect(described_class).to receive(:new).once
      expect(context).to receive(:close).once

      described_class.with_context(mongocrypt) do |_|
        # something here
      end
    end
  end
end
