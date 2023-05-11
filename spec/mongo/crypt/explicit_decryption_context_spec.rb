# frozen_string_literal: true
# rubocop:todo all

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::ExplicitDecryptionContext do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:credentials) { Mongo::Crypt::KMS::Credentials.new(kms_providers) }
  let(:mongocrypt) { Mongo::Crypt::Handle.new(credentials, logger: logger) }
  let(:context) { described_class.new(mongocrypt, io, value) }
  let(:logger) { nil }
  let(:io) { double("Mongo::ClientEncryption::IO") }

  # A binary string representing a value previously encrypted by libmongocrypt
  let(:encrypted_data) do
    "\x01\xDF2~\x89\xD2+N}\x84;i(\xE5\xF4\xBF \x024\xE5\xD2\n\x9E\x97\x9F\xAF\x9D\xC7\xC9\x1A\a\x87z\xAE_;r\xAC\xA9\xF6n\x1D\x0F\xB5\xB1#O\xB7\xCA\xEE$/\xF1\xFA\b\xA7\xEC\xDB\xB6\xD4\xED\xEAMw3+\xBBv\x18\x97\xF9\x99\xD5\x13@\x80y\n{\x19R\xD3\xF0\xA1C\x05\xF7)\x93\x9Bh\x8AA.\xBB\xD3&\xEA"
  end

  let(:value) do
    { 'v': BSON::Binary.new(encrypted_data, :ciphertext) }
  end

  describe '#initialize' do
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

    context 'when mongocrypt is initialized with Azure KMS provider options' do
      include_context 'with Azure kms_providers'

      it 'initializes context' do
        expect do
          context
        end.not_to raise_error
      end
    end

    context 'when mongocrypt is initialized with GCP KMS provider options' do
      include_context 'with GCP kms_providers'

      it 'initializes context' do
        expect do
          context
        end.not_to raise_error
      end
    end

    context 'when mongocrypt is initialized with KMIP KMS provider options' do
      include_context 'with KMIP kms_providers'

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
        ::Logger.new(STDOUT).tap do |logger|
          logger.level = ::Logger::DEBUG
        end
      end

      it 'receives log messages from libmongocrypt' do
        expect(logger).to receive(:debug).with(/mongocrypt_ctx_explicit_decrypt_init/)

        context
      end
    end
  end
end
