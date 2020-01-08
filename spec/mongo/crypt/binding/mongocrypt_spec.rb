require 'mongo'
require 'support/lite_constraints'
require 'mongo/crypt/helpers/mongo_crypt_spec_helper'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe 'Mongo::Crypt::Binding' do
  describe 'mongocrypt_t binding' do
    require_libmongocrypt

    after do
      Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
    end

    describe '#mongocrypt_new' do
      let(:mongocrypt) { Mongo::Crypt::Binding.mongocrypt_new }

      it 'returns a pointer' do
        expect(mongocrypt).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_setopt_kms_provider_local' do
      let(:mongocrypt) { Mongo::Crypt::Binding.mongocrypt_new }

      let(:binary) do
        p = FFI::MemoryPointer.new(key_bytes.size)
              .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, key_bytes)

        Mongo::Crypt::Binding.mongocrypt_binary_new_from_data(p, key_bytes.length)
      end

      after do
        Mongo::Crypt::Binding.mongocrypt_binary_destroy(binary)
      end

      context 'with valid key' do
        let(:key_bytes) { [114, 117, 98, 121] * 24 } # 96 bytes

        it 'returns true' do
          expect(Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, binary)).to be true
        end
      end

      context 'with invalid key' do
        let(:key_bytes) { [114, 117, 98, 121] * 23 } # NOT 96 bytes

        it 'returns false' do
          expect(Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, binary)).to be false
        end
      end
    end

    describe '#mongocrypt_init' do
      let(:key_bytes) { [114, 117, 98, 121] * 24 } # 96 bytes

      let(:binary) do
        p = FFI::MemoryPointer.new(key_bytes.size)
              .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, key_bytes)

        Mongo::Crypt::Binding.mongocrypt_binary_new_from_data(p, key_bytes.length)
      end

      let(:mongocrypt) do
        mongocrypt = Mongo::Crypt::Binding.mongocrypt_new
        Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, binary)

        mongocrypt
      end

      after do
        Mongo::Crypt::Binding.mongocrypt_binary_destroy(binary)
      end

      context 'with valid kms option' do
        before do
          MongoCryptSpecHelper.bind_crypto_hooks(mongocrypt)
        end

        it 'returns true' do
          expect(Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)).to be true
        end
      end

      context 'without binding crypto hooks' do
        it 'returns false' do
          expect(Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)).to be false
        end
      end

      context 'with invalid kms option' do
        before do
          MongoCryptSpecHelper.bind_crypto_hooks(mongocrypt)
        end

        let(:key_bytes) { [114, 117, 98, 121] * 23 } # NOT 96 bytes

        it 'returns false' do
          expect(Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)).to be false
        end
      end
    end

    describe '#mongocrypt_status' do
      let(:status) { Mongo::Crypt::Binding.mongocrypt_status_new }
      let(:mongocrypt) { mongocrypt = Mongo::Crypt::Binding.mongocrypt_new }

      after do
        Mongo::Crypt::Binding.mongocrypt_status_destroy(status)
      end

      context 'for a new mongocrypt_t object' do
        it 'returns an ok status' do
          Mongo::Crypt::Binding.mongocrypt_status(mongocrypt, status)
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status)).to eq(:ok)
        end
      end

      context 'for a mongocrypt_t object with invalid kms options' do
        let(:key_bytes) { [114, 117, 98, 121] * 23 } # NOT 96 bytes

        let(:binary) do
          p = FFI::MemoryPointer.new(key_bytes.size)
                .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, key_bytes)

          Mongo::Crypt::Binding.mongocrypt_binary_new_from_data(p, key_bytes.length)
        end

        after do
          Mongo::Crypt::Binding.mongocrypt_binary_destroy(binary)
        end

        it 'returns a error_client status' do
          Mongo::Crypt::Binding.mongocrypt_setopt_kms_provider_local(mongocrypt, binary)

          Mongo::Crypt::Binding.mongocrypt_status(mongocrypt, status)
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status)).to eq(:error_client)
        end
      end
    end
  end
end
