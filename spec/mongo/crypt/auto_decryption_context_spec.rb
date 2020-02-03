require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::AutoDecryptionContext do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:mongocrypt) { Mongo::Crypt::Handle.new(kms_providers, logger: logger) }
  let(:context) { described_class.new(mongocrypt, io, command) }

  let(:logger) { nil }

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
      context 'when mongocrypt is initialized with local KMS provider options' do
        include_context 'with local kms_providers'

        it 'initializes context' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'when mongocrypt is initialized with AWS KMS provider options' do
        include_context 'with AWS kms_providers'

        it 'initializes context' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with verbose logging' do
        include_context 'with local kms_providers'

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
