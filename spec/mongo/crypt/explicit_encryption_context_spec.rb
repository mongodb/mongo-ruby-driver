require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::ExplicitEncryptionContext do
  require_libmongocrypt

  let(:context) { described_class.new(mongocrypt, io, value, options) }

  let(:mongocrypt) do
    Mongo::Crypt::Handle.new({
      local: {
        key: Base64.encode64("ru\xfe\x00" * 24)
      }
    })
  end

  let(:io) { double("Mongo::ClientEncryption::IO") }
  let(:value) { { 'v': 'Hello, world!' }.to_bson.to_s }

  let(:algorithm) { 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic' }
  let(:key_id) { "]\xB1\xE1>\xD6\x85G\xCA\xBB\xA3`\e4\x06\xDA\x89" }

  let(:options) do
    {
      key_id: key_id,
      algorithm: algorithm
    }
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
      let(:key_id) { 'random string' }

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

      context 'with verbose logging' do
        before(:all) do
          # Logging from libmongocrypt requires the C library to be built with the -DENABLE_TRACE=ON
          # option; none of the pre-built packages on Evergreen have been built with logging enabled.
          #
          # It is still useful to be able to run these tests locally to confirm that logging is working
          # while debugging any problems.
          #
          # For now, skip this test by default and revisit once we have determined how we want to
          # package libmongocrypt with the Ruby driver.
          skip "These tests require libmongocrypt to be built with the '-DENABLE_TRACE=ON' cmake option." +
            " They also require the MONGOCRYPT_TRACE environment variable to be set to 'ON'."
        end

        before do
          @original_logger_level = Mongo::Logger.level
          Mongo::Logger.level = Logger::DEBUG
        end

        after do
          Mongo::Logger.level = @original_logger_level
        end

        let(:logger) { Mongo::Logger.logger }

        it 'receives log messages from libmongocrypt' do
          expect(logger).to receive(:debug).with(/mongocrypt_ctx_setopt_key_id/)
          expect(logger).to receive(:debug).with(/mongocrypt_ctx_setopt_algorithm/)
          expect(logger).to receive(:debug).with(/mongocrypt_ctx_explicit_encrypt_init/)

          context
        end
      end
    end
  end
end
