require 'mongo'
require 'support/lite_constraints'

RSpec.configure do |config|
  config.extend(LiteConstraints)
end

describe Mongo::Crypt::AutoDecryptionContext do
  require_libmongocrypt

  let(:mongocrypt) { Mongo::Crypt::Handle.new(kms_providers, logger: logger) }
  let(:context) { described_class.new(mongocrypt, io, command) }

  let(:logger) { nil }
  let(:kms_providers) do
    {
      local: {
        key: Base64.encode64("ru\xfe\x00" * 24)
      }
    }
  end

  let(:io) { double("Mongo::ClientEncryption::IO") }
  let(:command) do
    {
      "find": "test",
      "filter": {
          "ssn": "457-55-5462"
      }
    }
  end

  describe '#initialize' do
    context 'with valid options' do
      it 'does not raise an exception' do
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
          # package libmongocrypt with the Ruby driver (see: https://jira.mongodb.org/browse/RUBY-1966)
          skip "These tests require libmongocrypt to be built with the '-DENABLE_TRACE=ON' cmake option." +
            " They also require the MONGOCRYPT_TRACE environment variable to be set to 'ON'."
        end

        let(:logger) do
          ::Logger.new($stdout).tap do |logger|
            logger.level = ::Logger::DEBUG
          end
        end

        it 'receives log messages from libmongocrypt' do
          expect(logger).to receive(:debug).with(/mongocrypt_ctx_decrypt_init/)
          context
        end
      end
    end
  end
end
