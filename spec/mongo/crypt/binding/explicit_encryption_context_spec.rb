require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::ExplicitEncryptionContext do
  require_libmongocrypt

  let(:mongocrypt) { Mongo::Crypt::Binding.mongocrypt_new }
  let(:context) { described_class.new(mongocrypt, value, options) }

  let(:value) { BSON::Binary.new({ 'v': 'Hello, world!' }.to_bson.to_s) }

  let(:options) do
    {
      key_id: key_id,
      algorithm: algorithm
    }
  end

  let(:algorithm) { 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic' }
  let(:key_id) { BSON::Binary.new("]\xB1\xE1>\xD6\x85G\xCA\xBB\xA3`\e4\x06\xDA\x89") }

  before do
    Mongo::Crypt::Binding.mongocrypt_init(mongocrypt)
  end

  after do
    Mongo::Crypt::Binding.mongocrypt_destroy(mongocrypt)
  end

  describe '#initialize' do
    context 'with nil key_id option' do
      let(:key_id) { nil }

      it 'raises an exception' do
        expect do
          context
        end.to raise_error(ArgumentError, /:key_id option must not be nil/)
      end
    end

    context 'with invalid key_id' do
      let(:key_id) { BSON::Binary.new('random string') }

      it 'raises an exception' do
        expect do
          context
        end.to raise_error(Mongo::Error::CryptClientError, /expected 16 byte UUID/)
      end
    end

    context 'with nil algorithm' do
      let(:algorithm) { nil }

      it 'raises exception' do
        expect do
          context
        end.to raise_error(Mongo::Error::CryptClientError, /passed null algorithm/)
      end
    end

    context 'with invalid algorithm' do
      let(:algorithm) { 'unsupported-algorithm' }

      it 'raises an exception' do
        expect do
          context
        end.to raise_error(Mongo::Error::CryptClientError, /unsupported algorithm/)
      end
    end

    context 'with valid options' do
      it 'initializes context' do
        expect do
          context
        end.not_to raise_error
      end
    end
  end

  describe '#run_state_machine' do
    it 'encrypts the value' do
      context.run_state_machine
    end
  end
end
