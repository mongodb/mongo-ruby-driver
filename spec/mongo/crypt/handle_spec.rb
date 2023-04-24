# frozen_string_literal: true
# rubocop:todo all

require 'mongo'
require 'base64'
require 'spec_helper'

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
        schema_map_path: schema_map_path,
        bypass_query_analysis: bypass_query_analysis,
        crypt_shared_lib_path: crypt_shared_lib_path,
        crypt_shared_lib_required: crypt_shared_lib_required,
        explicit_encryption_only: explicit_encryption_only,
      )
    end

    let(:schema_map) do
      nil
    end

    let(:schema_map_path) do
      nil
    end

    let(:bypass_query_analysis) do
      nil
    end

    let(:crypt_shared_lib_path) do
      nil
    end

    let(:crypt_shared_lib_required) do
      nil
    end

    let(:explicit_encryption_only) do
      nil
    end

    shared_examples 'a functioning Mongo::Crypt::Handle' do
      context 'with valid schema map' do
        it 'does not raise an exception' do
          expect { handle }.not_to raise_error
        end
      end

      context 'with valid schema map in a file' do
        let(:schema_map_path) do
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
            expect { handle }.to raise_error(ArgumentError, /Cannot set both schema_map and schema_map_path options/)
          end
        end
      end

      context 'with invalid schema map' do
        let(:schema_map) { '' }

        it 'raises an exception' do
          expect { handle }.to raise_error(ArgumentError, /invalid schema_map; schema_map must be a Hash or nil/)
        end
      end

      context 'with nil schema map' do
        let(:schema_map) { nil }

        it 'does not raise an exception' do
          expect { handle }.not_to raise_error
        end
      end

      context 'with crypt_shared_lib_path' do
        min_server_version '6.0.0'

        context 'with correct path' do
          let(:crypt_shared_lib_path) do
            SpecConfig.instance.crypt_shared_lib_path
          end

          it 'loads the crypt shared lib' do
            expect(handle.crypt_shared_lib_version).not_to eq(0)
          end
        end

        context 'with incorrect path' do
          let(:crypt_shared_lib_path) do
            '/some/bad/path/mongo_crypt_v1.so'
          end

          it 'raises an exception' do
            expect { handle }.to raise_error(Mongo::Error::CryptError)
          end
        end
      end

      context 'with crypt_shared_lib_required' do
        min_server_version '6.0.0'

        context 'set to true' do
          let(:crypt_shared_lib_required) do
            true
          end

          context 'when shared lib is available' do
            let(:crypt_shared_lib_path) do
              SpecConfig.instance.crypt_shared_lib_path
            end

            it 'does not raise an exception' do
              expect { handle }.not_to raise_error
            end
          end

          context 'when shared lib is not available' do
            let(:crypt_shared_lib_path) do
              '/some/bad/path/mongo_crypt_v1.so'
            end

            it 'raises an exception' do
              expect { handle }.to raise_error(Mongo::Error::CryptError)
            end
          end
        end
      end

      context 'if bypass_query_analysis is true' do
        min_server_version '6.0.0'

        let(:bypass_query_analysis) do
          true
        end

        it 'does not load the crypt shared lib' do
          expect(Mongo::Crypt::Binding).not_to receive(:setopt_append_crypt_shared_lib_search_path)

          expect(handle.crypt_shared_lib_version).to eq(0)
        end
      end

      context 'if explicit_encryption_only is true' do
        min_server_version '6.0.0'

        let(:explicit_encryption_only) do
          true
        end

        it 'does not load the crypt shared lib' do
          expect(Mongo::Crypt::Binding).not_to receive(:setopt_append_crypt_shared_lib_search_path)

          expect(handle.crypt_shared_lib_version).to eq(0)
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

      context 'with empty AWS kms_providers' do
        let(:kms_providers) do
          {
            aws: {}
          }
        end

        it 'instructs libmongocrypt to handle empty AWS credentials' do
          expect(Mongo::Crypt::Binding).to receive(
            :setopt_use_need_kms_credentials_state
          ).once.and_call_original
          handle
        end
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
