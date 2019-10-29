require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::Binary do
  require_libmongocrypt

  let(:bytes) { [104, 101, 108, 108, 111] }
  let(:binary) { described_class.new(bytes) }

  describe '#initialize' do
    context 'with nil data' do
      it 'raises an exception' do
        expect do
          described_class.new(nil)
        end.to raise_error(Mongo::Error::CryptError, /Cannot create new Binary object/)
      end
    end

    context 'with valid data' do
      after do
        binary.close
      end

      it 'creates a new Mongo::Crypt::Binary object' do
        expect do
          binary
        end.not_to raise_error
      end
    end
  end

  describe '#to_bytes' do
    after do
      binary.close
    end

    it 'returns the string as a byte array' do
      expect(binary.to_bytes).to eq(bytes)
    end
  end

  describe '#self.with_binary' do
    before do
      allow(described_class)
        .to receive(:new)
        .and_return(binary)
    end

    context 'when yield errors' do
      it 'closes the created binary and raises the error' do
        expect(binary).to receive(:close).once

        expect do
          described_class.with_binary(bytes) do |bin|
            raise StandardError.new("an error")
          end
        end.to raise_error(StandardError, /an error/)
      end
    end

    it 'creates a new binary and closes it' do
      expect(binary).to receive(:close).once
      described_class.with_binary(bytes) { |bin| }
    end
  end
end
