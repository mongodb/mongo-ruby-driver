# frozen_string_literal: true
# rubocop:todo all

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::ExplicitEncryptionContext do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:credentials) { Mongo::Crypt::KMS::Credentials.new(kms_providers) }
  let(:mongocrypt) { Mongo::Crypt::Handle.new(credentials, logger: logger) }
  let(:context) { described_class.new(mongocrypt, io, value, options) }

  let(:logger) { nil }

  let(:io) { double("Mongo::ClientEncryption::IO") }
  let(:value) { { 'v': 'Hello, world!' } }

  let(:options) do
    {
      key_id: key_id,
      key_alt_name: key_alt_name,
      algorithm: algorithm
    }
  end

  describe '#initialize' do
    shared_examples 'a functioning ExplicitEncryptionContext' do
      context 'with nil key_id and key_alt_name options' do
        let(:key_id) { nil }
        let(:key_alt_name) { nil }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /:key_id and :key_alt_name options cannot both be nil/)
        end
      end

      context 'with both key_id and key_alt_name options' do
        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /:key_id and :key_alt_name options cannot both be present/)
        end
      end

      context 'with invalid key_id' do
        let(:key_id) { 'random string' }
        let(:key_alt_name) { nil }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /Expected the :key_id option to be a BSON::Binary object/)
        end
      end

      context 'with invalid key_alt_name' do
        let(:key_id) { nil }
        let(:key_alt_name) { 5 }

        it 'raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /key_alt_name option must be a String/)
        end
      end

      context 'with valid key_alt_name' do
        let(:key_id) { nil }

        context 'with nil algorithm' do
          let(:algorithm) { nil }

          it 'raises exception' do
            expect do
              context
            end.to raise_error(Mongo::Error::CryptError, /passed null algorithm/)
          end
        end

        context 'with invalid algorithm' do
          let(:algorithm) { 'unsupported-algorithm' }

          it 'raises an exception' do
            expect do
              context
            end.to raise_error(Mongo::Error::CryptError, /unsupported algorithm/)
          end
        end

        it 'initializes context' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with valid key_id' do
        let(:key_alt_name) { nil }

        context 'with nil algorithm' do
          let(:algorithm) { nil }

          it 'raises exception' do
            expect do
              context
            end.to raise_error(Mongo::Error::CryptError, /passed null algorithm/)
          end
        end

        context 'with invalid algorithm' do
          let(:algorithm) { 'unsupported-algorithm' }

          it 'raises an exception' do
            expect do
              context
            end.to raise_error(Mongo::Error::CryptError, /unsupported algorithm/)
          end
        end

        it 'initializes context' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with query_type' do
        let(:key_alt_name) { nil }

        it 'raises exception' do
          expect do
            described_class.new(
              mongocrypt,
              io,
              value,
              options.merge(query_type: "equality")
            )
          end.to raise_error(ArgumentError, /query_type is allowed only for "Indexed" or "RangePreview" algorithm/)
        end
      end

      context 'with contention_factor' do
        let(:key_alt_name) { nil }

        it 'raises exception' do
          expect do
            described_class.new(
              mongocrypt,
              io,
              value,
              options.merge(contention_factor: 10)
            )
          end.to raise_error(ArgumentError, /contention_factor is allowed only for "Indexed" or "RangePreview" algorithm/)
        end
      end

      context 'with Indexed algorithm' do
        let(:algorithm) do
          'Indexed'
        end

        let(:key_alt_name) do
          nil
        end

        it 'initializes context' do
          expect do
            described_class.new(
              mongocrypt,
              io,
              value,
              options.merge(contention_factor: 0)
            )
          end.not_to raise_error
        end

        context 'with query_type' do
          it 'initializes context' do
            expect do
              described_class.new(
                mongocrypt,
                io,
                value,
                options.merge(query_type: "equality", contention_factor: 0)
              )
            end.not_to raise_error
          end
        end

        context 'with contention_factor' do
          it 'initializes context' do
            expect do
              described_class.new(
                mongocrypt,
                io,
                value,
                options.merge(contention_factor: 10)
              )
            end.not_to raise_error
          end
        end
      end
    end

    context 'when mongocrypt is initialized with AWS KMS provider options' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'a functioning ExplicitEncryptionContext'
    end

    context 'when mongocrypt is initialized with Azure KMS provider options' do
      include_context 'with Azure kms_providers'
      it_behaves_like 'a functioning ExplicitEncryptionContext'
    end

    context 'when mongocrypt is initialized with GCP KMS provider options' do
      include_context 'with GCP kms_providers'
      it_behaves_like 'a functioning ExplicitEncryptionContext'
    end

    context 'when mongocrypt is initialized with KMIP KMS provider options' do
      include_context 'with KMIP kms_providers'
      it_behaves_like 'a functioning ExplicitEncryptionContext'
    end

    context 'when mongocrypt is initialized with local KMS provider options' do
      include_context 'with local kms_providers'
      it_behaves_like 'a functioning ExplicitEncryptionContext'
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

      let(:key_alt_name) { nil }
      let(:logger) do
        ::Logger.new(STDOUT).tap do |logger|
          logger.level = ::Logger::DEBUG
        end
      end

      it 'receives log messages from libmongocrypt' do
        expect(logger).to receive(:debug).with(/mongocrypt_ctx_setopt_key_id/)
        expect(logger).to receive(:debug).with(/mongocrypt_ctx_setopt_algorithm/)
        expect(logger).to receive(:debug).with(/mongocrypt_ctx_explicit_encrypt_init/)

        context
      end
    end
  end
end
