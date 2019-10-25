require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe 'Mongo::Crypt::Binding' do
  describe 'mongocrypt_status_t binding' do
    let(:status) { Mongo::Crypt::Binding.mongocrypt_status_new }
    let(:string) { "Operation successful" }

    let(:status_with_info) do
      Mongo::Crypt::Binding.mongocrypt_status_set(
        status,
        :ok,
        200,
        string,
        string.length + 1
      )

      status
    end

    after do
      Mongo::Crypt::Binding.mongocrypt_status_destroy(status)
    end

    describe '#mongocrypt_status_new' do
      it 'returns a pointer' do
        expect(status).to be_a_kind_of(FFI::Pointer)
      end
    end

    describe '#mongocrypt_status_type' do
      context 'when status has no type' do
        it 'returns 0' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status)).to eq(0)
        end
      end

      context 'when status has type' do
        it 'returns type' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status_with_info)).to eq(:ok)
        end
      end
    end

    describe '#mongocrypt_status_code' do
      context 'when status has no code' do
        it 'returns 0' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_code(status)).to eq(0)
        end
      end

      context 'when status has code' do
        it 'returns code' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_code(status_with_info)).to eq(200)
        end
      end
    end
  end
end
