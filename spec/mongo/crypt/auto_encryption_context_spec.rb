# frozen_string_literal: true
# rubocop:todo all

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::AutoEncryptionContext do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:credentials) { Mongo::Crypt::KMS::Credentials.new(kms_providers) }
  let(:mongocrypt) { Mongo::Crypt::Handle.new(credentials, logger: logger) }
  let(:context) { described_class.new(mongocrypt, io, db_name, command) }

  let(:logger) { nil }

  let(:io) { double("Mongo::ClientEncryption::IO") }
  let(:db_name) { 'admin' }
  let(:command) do
    {
      "find": "test",
      "filter": {
          "ssn": "457-55-5462"
      }
    }
  end

  describe '#initialize' do
    shared_examples 'a functioning AutoEncryptionContext' do
      context 'with valid command' do
        it 'initializes context' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with invalid command' do
        let(:command) do
          {
            incorrect_key: 'value'
          }
        end

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(/command not supported for auto encryption: incorrect_key/)
        end
      end

      context 'with nil command' do
        let(:command) { nil }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(Mongo::Error::CryptError, /Attempted to pass nil data to libmongocrypt/)
        end
      end

      context 'with non-document command' do
      let(:command) { 'command-to-encrypt' }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(Mongo::Error::CryptError, /Attempted to pass invalid data to libmongocrypt/)
        end
      end
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning AutoEncryptionContext'
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning AutoEncryptionContext'
    end

    context 'with Azure KMS providers' do
      include_context 'with Azure kms_providers'
      it_behaves_like 'a functioning AutoEncryptionContext'
    end

    context 'with GCP KMS providers' do
      include_context 'with GCP kms_providers'
      it_behaves_like 'a functioning AutoEncryptionContext'
    end

    context 'with KMIP KMS providers' do
      include_context 'with KMIP kms_providers'
      it_behaves_like 'a functioning AutoEncryptionContext'
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
        expect(logger).to receive(:debug).with(/mongocrypt_ctx_encrypt_init/)
        context
      end
    end
  end
end
