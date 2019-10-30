require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::Status do
  require_libmongocrypt

  let(:status) { Mongo::Crypt::Status.new }

  let(:label) { :error_client }
  let(:code) { 401 }
  let(:message) { 'Unauthorized' }

  let(:status_with_info) do
    status.update(label, code, message)
  end

  describe '#initialize' do
    after do
      status.close
    end

    it 'doesn\'t throw an error' do
      expect { status }.not_to raise_error
    end
  end

  describe '#set' do
    after do
      status.close
    end

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
    after do
      status.close
    end

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
    after do
      status.close
    end

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
    after do
      status.close
    end

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
    after do
      status.close
    end

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

  describe '#self.with_status' do
    before do
      allow(described_class)
        .to receive(:new)
        .and_return(status)
    end

    context 'when yield errors' do
      it 'closes the created status and raises the error' do
        expect(status).to receive(:close).once

        expect do
          described_class.with_status do |s|
            raise StandardError.new("an error")
          end
        end.to raise_error(StandardError, /an error/)
      end

      it 'creates a new status and closes it' do
        expect(status).to receive(:close).once

        described_class.with_status do |s|
          expect(s.ok?).to be true
        end
      end
    end
  end
end
