require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::Binary do
  require_libmongocrypt

  let(:data) { 'I love Ruby' }
  let(:binary) { described_class.new(data) }

  describe '#initialize' do
    context 'with nil data' do
      it 'creates a new Mongo::Crypt::Binary object' do
        expect do
          binary
        end.not_to raise_error
      end
    end

    context 'with valid data' do
      it 'creates a new Mongo::Crypt::Binary object' do
        expect do
          binary
        end.not_to raise_error
      end
    end
  end

  describe '#to_bytes' do
    it 'returns the string as a byte array' do
      expect(binary.to_bytes).to eq(data.unpack("C*"))
    end
  end

  describe '#to_string' do
    it 'returns the original string' do
      expect(binary.to_string).to eq(data)
    end
  end
end
