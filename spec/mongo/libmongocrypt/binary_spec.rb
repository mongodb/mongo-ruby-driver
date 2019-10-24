require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Libmongocrypt::Binary do
  require_libmongocrypt

  let(:string) { 'hello' }
  let(:string_to_bytes) { [104, 101, 108, 108, 111] }
  let(:binary) { described_class.new(string) }

  after(:each) do
    binary.close if binary
  end

  describe '#initialize' do
    context 'with nil data string' do
      it 'raises an exception' do
        expect do
          described_class.new(nil)
        end.to raise_error(Mongo::Libmongocrypt::MongocryptError, /Cannot create new Binary object/)
      end
    end

    it 'creates a new Mongo::Libmongocrypt::Binary object' do
      expect do
        binary
      end.not_to raise_error
    end
  end

  describe '#to_bytes' do
    it 'returns the string as a byte array' do
      expect(binary.to_bytes).to eq(string_to_bytes)
    end
  end

  describe '#close' do
    it 'removes references to the binary data' do
      binary.close
      expect(binary.bytes).to be_empty
    end
  end
end
