# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Crypt::Status do
  require_libmongocrypt

  let(:status) { described_class.new }

  let(:label) { :error_client }
  let(:code) { 401 }
  let(:message) { 'Unauthorized' }

  let(:status_with_info) do
    status.update(label, code, message)
  end

  describe '#initialize' do
    it 'doesn\'t throw an error' do
      expect { status }.not_to raise_error
    end
  end

  describe '#self.from_pointer' do
    let(:pointer) { Mongo::Crypt::Binding.mongocrypt_status_new }
    let(:status) { described_class.from_pointer(pointer) }

    after do
      Mongo::Crypt::Binding.mongocrypt_status_destroy(pointer)
    end

    it 'creates a status from the pointer passed in' do
      expect do
        status
      end.not_to raise_error

      expect(status.ref).to eq(pointer)
    end
  end

  describe '#update' do
    context 'with invalid label' do
      it 'raises an exception' do
        expect do
          status.update(:random_label, 0, '')
        end.to raise_error(ArgumentError, /random_label is an invalid value for a Mongo::Crypt::Status label/)
      end

      it 'works with an empty message' do
        status.update(:ok, 0, '')
        expect(status.message).to eq('')
      end
    end
  end

  describe '#label' do
    context 'new status' do
      it 'returns :ok' do
        expect(status.label).to eq(:ok)
      end
    end

    context 'status with info' do
      it 'returns label' do
        expect(status_with_info.label).to eq(label)
      end
    end
  end

  describe '#code' do
    context 'new status' do
      it 'returns 0' do
        expect(status.code).to eq(0)
      end
    end

    context 'status with info' do
      it 'returns code' do
        expect(status_with_info.code).to eq(code)
      end
    end
  end

  describe '#message' do
    context 'new status' do
      it 'returns an empty string' do
        expect(status.message).to eq('')
      end
    end

    context 'status with info' do
      it 'returns a message' do
        expect(status_with_info.message).to eq(message)
      end
    end
  end

  describe '#ok?' do
    context 'new status' do
      it 'returns true' do
        expect(status.ok?).to be true
      end
    end

    context 'status with info' do
      it 'returns false' do
        expect(status_with_info.ok?).to be false
      end
    end
  end

  describe '#crypt_error' do
    context 'when status is ok' do
      before do
        status.update(:ok, 0, '')
      end

      it 'does not raise exception' do
        expect do
          status.raise_crypt_error
        end.not_to raise_error
      end
    end

    context 'when status is :error_kms' do
      before do
        status.update(:error_kms, 100, 'KMS error message')
      end

      it 'raises exception' do
        expect do
          status.raise_crypt_error
        end.to raise_error(Mongo::Error::KmsError, 'KMS error message (libmongocrypt error code 100)')
      end
    end

    context 'when status is error client' do
      before do
        status.update(:error_client, 2, 'Client Error')
      end

      it 'raises exception' do
        expect do
          status.raise_crypt_error
        end.to raise_error(Mongo::Error::CryptError, 'Client Error (libmongocrypt error code 2)')
      end
    end
  end
end
