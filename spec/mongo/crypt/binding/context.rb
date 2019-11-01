require 'mongo'
require 'support/lite_constraints'
require 'byebug' # TODO: remove

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe 'Mongo::Crypt::Binding' do
  describe 'mongocrypt_ctx_t bindings' do
    require_libmongocrypt

    let(:context) { Mongo::Crypt::Binding.mongocrypt_ctx_new(mongocrypt) }
    let(:mongocrypt) { Mongo::Crypt::Binding.mongocrypt_new }

    describe '#mongocrypt_ctx_new' do
      after do
        Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
        Mongo::Crypt::Binding.mongocrypt_ctx_destroy(context)
      end

      it 'returns a pointer' do
        expect(context).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_ctx_status' do
      let(:status) { Mongo::Crypt::Binding.mongocrypt_status_new }

      after do
        Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
        Mongo::Crypt::Binding.mongocrypt_ctx_destroy(context)
        Mongo::Crypt::Binding.mongocrypt_status_destroy(status)
      end

      context 'for a new mongocrypt_ctx_t object' do
        it 'returns an ok status' do
          Mongo::Crypt::Binding.mongocrypt_ctx_status(context, status)
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status)).to eq(:ok)
        end
      end
    end


    describe '#mongocrypt_ctx_setopt_key_id' do
      # 16-byte binary uuid string
      let(:uuid) { "\xDEd\x00\xDC\x0E\xF8J\x99\x97\xFA\xCC\x04\xBF\xAA\x00\xF5" }
      let(:bytes) { uuid.unpack('C*') }
      let(:binary) do
        p = FFI::MemoryPointer
          .new(bytes.size)
          .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, bytes)

        Mongo::Crypt::Binding.mongocrypt_binary_new_from_data(p, bytes.length)
      end

      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_setopt_key_id(context, binary)
      end

      before do
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      after do
        Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
        Mongo::Crypt::Binding.mongocrypt_binary_destroy(binary)
        Mongo::Crypt::Binding.mongocrypt_ctx_destroy(context)
      end

      context 'with valid key id' do
        it 'returns true' do
          expect(result).to be true
        end
      end
    end

    describe '#mongocrypt_ctx_setopt_algorithm' do
      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_setopt_algorithm(
          context,
          algo,
          -1
        )
      end

      before do
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      after do
        Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
        Mongo::Crypt::Binding.mongocrypt_ctx_destroy(context)
      end

      context 'with deterministic algorithm' do
        let(:algo) { 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic' }

        it 'returns true' do
          expect(result).to be true
        end
      end

      context 'with random algorithm' do
        let(:algo) { 'AEAD_AES_256_CBC_HMAC_SHA_512-Random' }

        it 'returns true' do
          expect(result).to be true
        end
      end

      context 'with invalid algorithm' do
        let(:algo) { 'fake-algorithm' }

        it 'returns false' do
          expect(result).to be false
        end
      end
    end

    describe '#mongocrypt_ctx_setopt_masterkey_local' do
      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_setopt_masterkey_local(context)
      end

      before do
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      after do
        Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
        Mongo::Crypt::Binding.mongocrypt_ctx_destroy(context)
      end

      context 'with valid local master key' do
        it 'returns true' do
          expect(result).to be true
        end
      end
    end
  end
end
