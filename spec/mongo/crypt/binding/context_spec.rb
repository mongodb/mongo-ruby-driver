# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require_relative '../helpers/mongo_crypt_spec_helper'

shared_context 'initialized for data key creation' do
  let(:master_key) { "ru\xfe\x00" * 24 }

  let(:kms_providers) do
    BSON::Document.new({
      local: {
        key: BSON::Binary.new(master_key, :generic),
      }
    })
  end

  let(:binary) do
    MongoCryptSpecHelper.mongocrypt_binary_t_from(kms_providers.to_bson.to_s)
  end

  let(:key_document) do
    MongoCryptSpecHelper.mongocrypt_binary_t_from(
      BSON::Document.new({provider: 'local'}).to_bson.to_s)
  end

  before do
    Mongo::Crypt::Binding.mongocrypt_setopt_kms_providers(mongocrypt, binary)
    MongoCryptSpecHelper.bind_crypto_hooks(mongocrypt)
    Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)

    Mongo::Crypt::Binding.mongocrypt_ctx_setopt_key_encryption_key(context, key_document)
  end

  after do
    Mongo::Crypt::Binding.mongocrypt_binary_destroy(key_document)
    Mongo::Crypt::Binding.mongocrypt_binary_destroy(binary)
  end
end

shared_context 'initialized for explicit encryption' do
  # TODO: replace with code showing how to generate this value
  let(:key_id) { "\xDEd\x00\xDC\x0E\xF8J\x99\x97\xFA\xCC\x04\xBF\xAA\x00\xF5" }
  let(:key_id_binary) { MongoCryptSpecHelper.mongocrypt_binary_t_from(key_id) }

  let(:value) do
    { 'v': 'Hello, world!' }.to_bson.to_s
  end

  let(:value_binary) { MongoCryptSpecHelper.mongocrypt_binary_t_from(value) }

  before do
    MongoCryptSpecHelper.bind_crypto_hooks(mongocrypt)
    Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)

    Mongo::Crypt::Binding.mongocrypt_ctx_setopt_key_id(context, key_id_binary)
    Mongo::Crypt::Binding.mongocrypt_ctx_setopt_algorithm(
      context,
      'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
      -1
    )
  end

  after do
    Mongo::Crypt::Binding.mongocrypt_binary_destroy(key_id_binary)
    Mongo::Crypt::Binding.mongocrypt_binary_destroy(value_binary)
  end
end

describe 'Mongo::Crypt::Binding' do
  describe 'mongocrypt_ctx_t bindings' do
    require_libmongocrypt
    fails_on_jruby

    let(:mongocrypt) { Mongo::Crypt::Binding.mongocrypt_new }
    let(:context) { Mongo::Crypt::Binding.mongocrypt_ctx_new(mongocrypt) }

    after do
      Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
      Mongo::Crypt::Binding.mongocrypt_ctx_destroy(context)
    end

    describe '#mongocrypt_ctx_new' do
      it 'returns a pointer' do
        expect(context).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_ctx_status' do
      let(:status) { Mongo::Crypt::Binding.mongocrypt_status_new }

      after do
        Mongo::Crypt::Binding.mongocrypt_status_destroy(status)
      end

      context 'for a new mongocrypt_ctx_t object' do
        it 'returns an ok status' do
          Mongo::Crypt::Binding.mongocrypt_ctx_status(context, status)
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status)).to eq(:ok)
        end
      end
    end

    describe '#mongocrypt_ctx_datakey_init' do
      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_datakey_init(context)
      end

      context 'a master key option and KMS provider have been set' do
        include_context 'initialized for data key creation'

        it 'returns true' do
          expect(result).to be true
        end
      end
    end

    describe '#mongocrypt_ctx_setopt_key_id' do
      let(:binary) { MongoCryptSpecHelper.mongocrypt_binary_t_from(uuid) }

      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_setopt_key_id(context, binary)
      end

      before do
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      after do
        Mongo::Crypt::Binding.mongocrypt_binary_destroy(binary)
      end

      context 'with valid key id' do
        # 16-byte binary uuid string
        # TODO: replace with code showing how to generate this value
        let(:uuid) { "\xDEd\x00\xDC\x0E\xF8J\x99\x97\xFA\xCC\x04\xBF\xAA\x00\xF5" }

        it 'returns true' do
          expect(result).to be true
        end
      end

      context 'with invalid key id' do
        # invalid uuid string -- a truncated string of bytes
        let(:uuid) { "\xDEd\x00\xDC\x0E\xF8J\x99\x97\xFA\xCC\x04\xBF" }

        it 'returns false' do
          expect(result).to be false
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

      context 'with nil algorithm' do
        let(:algo) { nil }

        it 'returns false' do
          expect(result).to be false
        end
      end
    end

    describe '#mongocrypt_ctx_explicit_encrypt_init' do
      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_explicit_encrypt_init(context, value_binary)
      end

      context 'a key_id and algorithm have been set' do
        include_context 'initialized for explicit encryption'

        it 'returns true' do
          expect(result).to be true
        end
      end
    end

    describe '#mongocrypt_ctx_mongo_op' do
      context 'ctx is initialized for explicit encryption' do
        include_context 'initialized for explicit encryption'

        before do
          Mongo::Crypt::Binding.mongocrypt_ctx_explicit_encrypt_init(context, value_binary)
        end

        let(:out_binary) { Mongo::Crypt::Binding.mongocrypt_binary_new }
        let(:result) { Mongo::Crypt::Binding.mongocrypt_ctx_mongo_op(context, out_binary) }

        after do
          Mongo::Crypt::Binding.mongocrypt_binary_destroy(out_binary)
        end

        it 'returns a BSON document' do
          expect(result).to be true

          data = Mongo::Crypt::Binding.mongocrypt_binary_data(out_binary)
          len = Mongo::Crypt::Binding.mongocrypt_binary_len(out_binary)

          response = data.get_array_of_uint8(0, len).pack('C*')
          expect(response).to be_a_kind_of(String)
        end
      end
    end

    describe '#mongocrypt_ctx_state' do
      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_state(context)
      end

      context 'the mongocrypt_ctx has been properly initialized' do
        include_context 'initialized for data key creation'

        before do
          Mongo::Crypt::Binding.mongocrypt_ctx_datakey_init(context)
        end

        it 'returns ready state' do
          expect(result).to eq(:ready)
        end
      end
    end

    describe '#mongocrypt_ctx_setopt_query_type' do
      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_setopt_query_type(
          context,
          query_type,
          -1
        )
      end

      before do
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      context 'with equality query type' do
        let(:query_type) do
          "equality"
        end

        it 'returns true' do
          expect(result).to be true
        end
      end
    end

    describe '#mongocrypt_ctx_setopt_contention_factor' do
      let(:result) do
        Mongo::Crypt::Binding.mongocrypt_ctx_setopt_contention_factor(
          context,
          contention_factor
        )
      end

      before do
        Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
      end

      context 'with non zero contention factor' do
        let(:contention_factor) do
          10
        end

        it 'returns true' do
          expect(result).to be true
        end
      end
    end
  end
end
