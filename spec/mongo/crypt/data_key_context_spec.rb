# frozen_string_literal: true
# rubocop:todo all

require 'mongo'
require 'base64'
require 'lite_spec_helper'

describe Mongo::Crypt::DataKeyContext do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:credentials) { Mongo::Crypt::KMS::Credentials.new(kms_providers) }

  let(:kms_tls_options) do
    {}
  end

  let(:mongocrypt) do
    Mongo::Crypt::Handle.new(credentials, kms_tls_options)
  end

  let(:io) { double("Mongo::Crypt::EncryptionIO") }

  let(:key_alt_names) { [] }

  let(:context) { described_class.new(mongocrypt, io, key_document, key_alt_names, nil) }

  describe '#initialize' do
    shared_examples 'it properly sets key_alt_names' do
      context 'with one key_alt_names' do
        let(:key_alt_names) { ['keyAltName1'] }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with multiple key_alt_names' do
        let(:key_alt_names) { ['keyAltName1', 'keyAltName2'] }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with empty key_alt_names' do
        let(:key_alt_names) { [] }

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with invalid key_alt_names' do
        let(:key_alt_names) { ['keyAltName1', 3] }

        it 'does raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /All values of the :key_alt_names option Array must be Strings/)
        end
      end

      context 'with non-array key_alt_names' do
        let(:key_alt_names) { "keyAltName1" }

        it 'does raises an exception' do
          expect do
            context
          end.to raise_error(ArgumentError, /key_alt_names option must be an Array/)
        end
      end
    end

    context 'with aws kms provider' do
      include_context 'with AWS kms_providers'

      let(:key_document) do
        Mongo::Crypt::KMS::MasterKeyDocument.new(
          'aws',
          { master_key: { region: 'us-east-2', key: 'arn' } }
        )
      end

      it_behaves_like 'it properly sets key_alt_names'

      context 'with valid options' do
        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end

      context 'with valid endpoint' do
        let(:key_document) do
          Mongo::Crypt::KMS::MasterKeyDocument.new(
            'aws',
            {
              master_key: {
                region: 'us-east-2',
                key: 'arn',
                endpoint: 'kms.us-east-2.amazonaws.com:443'
              }
            }
          )
        end

        it 'does not raise an exception' do
          expect do
            context
          end.not_to raise_error
        end
      end
    end
  end

  describe '#run_state_machine' do
    # TODO: test with AWS KMS provider

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      let(:key_document) do
        Mongo::Crypt::KMS::MasterKeyDocument.new(
          'local',
          {
            master_key: { key: 'MASTER-KEY' }
          }
        )
      end

      it 'creates a data key' do
        expect(context.run_state_machine).to be_a_kind_of(Hash)
      end
    end
  end
end
