# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe 'Mongo::Crypt::Binding' do
  describe 'mongocrypt_status_t binding' do
    require_libmongocrypt

    let(:status) { Mongo::Crypt::Binding.mongocrypt_status_new }
    let(:message) { "Operation unauthorized" }

    let(:status_with_info) do
      Mongo::Crypt::Binding.mongocrypt_status_set(
        status,
        :error_client,
        401,
        message,
        message.length + 1
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
        it 'returns :ok/0' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status)).to eq(:ok)
        end
      end

      context 'when status has type' do
        it 'returns type' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_type(status_with_info)).to eq(:error_client)
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
          expect(Mongo::Crypt::Binding.mongocrypt_status_code(status_with_info)).to eq(401)
        end
      end
    end

    describe '#mongocrypt_status_message' do
      context 'when status has no message' do
        it 'returns nil' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_message(status, nil)).to eq(nil)
        end
      end

      context 'when status has message' do
        it 'returns message' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_message(status_with_info, nil)).to eq(message)
        end
      end
    end

    describe '#mongocrypt_status_ok' do
      context 'when status_type is not ok' do
        it 'returns false' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_ok(status_with_info)).to be false
        end
      end

      context 'when status_type is ok' do
        let(:message) { 'Operation successful' }
        let(:status_with_info) do
          Mongo::Crypt::Binding.mongocrypt_status_set(status, :ok, 200, message, message.length + 1)
          status
        end

        it 'returns true' do
          expect(Mongo::Crypt::Binding.mongocrypt_status_ok(status_with_info)).to be true
        end
      end
    end
  end
end
