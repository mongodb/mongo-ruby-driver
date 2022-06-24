# frozen_string_literal: true
# encoding: utf-8

require 'mongo'
require 'base64'
require 'lite_spec_helper'

describe Mongo::Crypt::Handle do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  describe '#initialize' do
    let(:credentials) { Mongo::Crypt::KMS::Credentials.new(kms_providers) }
    let(:kms_tls_options) { {} }
    let(:handle) do
      described_class.new(
        credentials,
        kms_tls_options,
        schema_map: schema_map,
        schema_map_file: schema_map_file,
      )
    end

    let(:schema_map) do
      nil
    end

    let(:schema_map_file) do
      nil
    end

    shared_examples 'a functioning Mongo::Crypt::Handle' do
      context 'with valid schema map' do
        it 'does not raise an exception' do
          expect { handle }.not_to raise_error
        end
      end

      context 'with valid schema map in a file' do
        let(:schema_map_file) do
          schema_map_file_path
        end

        context 'without schema_map set' do
          let(:schema_map) do
            nil
          end

          it 'does not raise an exception' do
            expect { handle }.not_to raise_error
          end
        end

        context 'with schema_map set' do
          it 'raises an exception' do
            expect { handle }.git adto raise_error(ArgumentError, /Cannot set both schema_map and schema_map_file options/)
          end
        end
      end

      context 'with invalid schema map' do
        let(:schema_map) { '' }

        it 'raises an exception' do
          expect { handle }.to raise_error(ArgumentError, /invalid schema_map; schema_map must a Hash or nil/)
        end
      end

      context 'with nil schema map' do
        let(:schema_map) { nil }

        it 'does not raise an exception' do
          expect { handle }.not_to raise_error
        end
      end
    end

    context 'local' do
      context 'with invalid local kms master key' do
        let(:kms_providers) do
          {
            local: {
              key: 'ruby' * 23 # NOT 96 bytes
            }
          }
        end

        it 'raises an exception' do
          expect { handle }.to raise_error(Mongo::Error::CryptError, 'local key must be 96 bytes (libmongocrypt error code 1)')
        end
      end

      context 'with valid local kms_providers' do
        include_context 'with local kms_providers'
        it_behaves_like 'a functioning Mongo::Crypt::Handle'
      end

    end

    context 'AWS' do
      context 'with valid AWS kms_providers' do
        include_context 'with AWS kms_providers'
        it_behaves_like 'a functioning Mongo::Crypt::Handle'
      end
    end

    context 'Azure' do
      context 'with valid azure kms_providers' do
        include_context 'with Azure kms_providers'
        it_behaves_like 'a functioning Mongo::Crypt::Handle'
      end
    end

    context 'GCP' do
      context 'with valid gcp kms_providers' do
        include_context 'with GCP kms_providers'
        it_behaves_like 'a functioning Mongo::Crypt::Handle'
      end
    end

    context 'GCP with PEM private key' do
      require_mri

      context 'with valid gcp kms_providers' do
        include_context 'with GCP kms_providers'

        let(:kms_providers) do
          {
            gcp: {
              email: SpecConfig.instance.fle_gcp_email,
              private_key: OpenSSL::PKey.read(
                Base64.decode64(SpecConfig.instance.fle_gcp_private_key)
              ).export,
            }
          }
        end

        it_behaves_like 'a functioning Mongo::Crypt::Handle'
      end
    end

    context 'KMIP' do
      context 'with valid kmip kms_providers' do
        include_context 'with KMIP kms_providers'
        it_behaves_like 'a functioning Mongo::Crypt::Handle'
      end
    end
  end
end
