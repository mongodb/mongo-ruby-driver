# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: KMS TLS Options Tests' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options
      )
    end

    let(:client_encryption_no_client_cert) do
      Mongo::ClientEncryption.new(
        client,
        {
          kms_providers: {
            aws: {
              access_key_id: SpecConfig.instance.fle_aws_key,
              secret_access_key: SpecConfig.instance.fle_aws_secret
            },
            azure: {
              tenant_id: SpecConfig.instance.fle_azure_tenant_id,
              client_id: SpecConfig.instance.fle_azure_client_id,
              client_secret: SpecConfig.instance.fle_azure_client_secret,
              identity_platform_endpoint: "127.0.0.1:8002"
            },
            gcp: {
              email: SpecConfig.instance.fle_gcp_email,
              private_key: SpecConfig.instance.fle_gcp_private_key,
              endpoint: "127.0.0.1:8002"
            },
            kmip: {
              endpoint: "127.0.0.1:5698"
            }
          },
          kms_tls_options: {
            aws: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            azure: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            gcp: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            kmip: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            }
          },
          key_vault_namespace: 'keyvault.datakeys',
        },
      )
    end

    let(:client_encryption_with_tls) do
      Mongo::ClientEncryption.new(
        client,
        {
          kms_providers: {
            aws: {
              access_key_id: SpecConfig.instance.fle_aws_key,
              secret_access_key: SpecConfig.instance.fle_aws_secret
            },
            azure: {
              tenant_id: SpecConfig.instance.fle_azure_tenant_id,
              client_id: SpecConfig.instance.fle_azure_client_id,
              client_secret: SpecConfig.instance.fle_azure_client_secret,
              identity_platform_endpoint: "127.0.0.1:8002"
            },
            gcp: {
              email: SpecConfig.instance.fle_gcp_email,
              private_key: SpecConfig.instance.fle_gcp_private_key,
              endpoint: "127.0.0.1:8002"
            },
            kmip: {
              endpoint: "127.0.0.1:5698"
            }
          },
          kms_tls_options: {
            aws: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file,
              ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
              ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
            },
            azure: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file,
              ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
              ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
            },
            gcp: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file,
              ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
              ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
            },
            kmip: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file,
              ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
              ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
            }
          },
          key_vault_namespace: 'keyvault.datakeys',
        },
      )
    end

    let(:client_encryption_expired) do
      Mongo::ClientEncryption.new(
        client,
        {
          kms_providers: {
            aws: {
              access_key_id: SpecConfig.instance.fle_aws_key,
              secret_access_key: SpecConfig.instance.fle_aws_secret
            },
            azure: {
              tenant_id: SpecConfig.instance.fle_azure_tenant_id,
              client_id: SpecConfig.instance.fle_azure_client_id,
              client_secret: SpecConfig.instance.fle_azure_client_secret,
              identity_platform_endpoint: "127.0.0.1:8000"
            },
            gcp: {
              email: SpecConfig.instance.fle_gcp_email,
              private_key: SpecConfig.instance.fle_gcp_private_key,
              endpoint: "127.0.0.1:8000"
            },
            kmip: {
              endpoint: "127.0.0.1:8000"
            }
          },
          kms_tls_options: {
            aws: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            azure: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            gcp: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            kmip: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            }
          },
          key_vault_namespace: 'keyvault.datakeys',
        },
      )
    end

    let(:client_encryption_invalid_hostname) do
      Mongo::ClientEncryption.new(
        client,
        {
          kms_providers: {
            aws: {
              access_key_id: SpecConfig.instance.fle_aws_key,
              secret_access_key: SpecConfig.instance.fle_aws_secret
            },
            azure: {
              tenant_id: SpecConfig.instance.fle_azure_tenant_id,
              client_id: SpecConfig.instance.fle_azure_client_id,
              client_secret: SpecConfig.instance.fle_azure_client_secret,
              identity_platform_endpoint: "127.0.0.1:8001"
            },
            gcp: {
              email: SpecConfig.instance.fle_gcp_email,
              private_key: SpecConfig.instance.fle_gcp_private_key,
              endpoint: "127.0.0.1:8001"
            },
            kmip: {
              endpoint: "127.0.0.1:8001"
            }
          },
          kms_tls_options: {
            aws: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            azure: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            gcp: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            },
            kmip: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
            }
          },
          key_vault_namespace: 'keyvault.datakeys',
        },
      )
    end

    # We do noy use shared examples for AWS because of the way we pass endpoint.
    context 'AWS' do
      let(:master_key_template) do
        {
          region: "us-east-1",
          key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
        }
      end

      context 'with no client certificate' do
        it 'TLS handshake failed' do
          expect do
            client_encryption_no_client_cert.create_data_key(
              'aws',
              {
                master_key: master_key_template.merge({endpoint: "127.0.0.1:8002"})
              }
            )
          end.to raise_error(Mongo::Error::KmsError, /(certificate_required|SocketError|ECONNRESET)/)
        end
      end

      context 'with valid certificate' do
        it 'TLS handshake passes' do
          expect do
            client_encryption_with_tls.create_data_key(
              'aws',
              {
                master_key: master_key_template.merge({endpoint: "127.0.0.1:8002"})
             }
            )
          end.to raise_error(Mongo::Error::KmsError, /libmongocrypt error code/)
        end
      end

      context 'with expired server certificate' do
        let(:error_regex) do
          if BSON::Environment.jruby?
            /certificate verify failed/
          else
            /certificate has expired/
          end
        end

        it 'TLS handshake failed' do
          expect do
            client_encryption_expired.create_data_key(
              'aws',
              {
                master_key: master_key_template.merge({endpoint: "127.0.0.1:8000"})
              }
            )
          end.to raise_error(Mongo::Error::KmsError, error_regex)
        end
      end

      context 'with server certificate with invalid hostname' do
        let(:error_regex) do
          if BSON::Environment.jruby?
            /TLS handshake failed due to a hostname mismatch/
          else
            /certificate verify failed/
          end
        end

        it 'TLS handshake failed' do
          expect do
            client_encryption_invalid_hostname.create_data_key(
              'aws',
              {
                master_key: master_key_template.merge({endpoint: "127.0.0.1:8001"})
              }
            )
          end.to raise_error(Mongo::Error::KmsError, error_regex)
        end
      end
    end

    shared_examples 'it respect KMS TLS options' do
      context 'with no client certificate' do
        it 'TLS handshake failed' do
          expect do
            client_encryption_no_client_cert.create_data_key(
              kms_provider,
              {
                master_key: master_key
             }
            )
          end.to raise_error(Mongo::Error::KmsError, /(certificate_required|SocketError|ECONNRESET)/)
        end
      end

      context 'with valid certificate' do
        it 'TLS handshake passes' do
          if should_raise_with_tls
            expect do
              client_encryption_with_tls.create_data_key(
                kms_provider,
                {
                  master_key: master_key
              }
              )
            end.to raise_error(Mongo::Error::KmsError, /libmongocrypt error code/)
          else
            expect do
              client_encryption_with_tls.create_data_key(
                kms_provider,
                {
                  master_key: master_key
              }
              )
            end.not_to raise_error
          end
        end

        it 'raises KmsError directly without wrapping CryptError' do
          if should_raise_with_tls
            begin
              client_encryption_with_tls.create_data_key(
                kms_provider,
                {
                  master_key: master_key
              }
              )
            rescue Mongo::Error::KmsError => exc
              exc.message.should =~ /Error when connecting to KMS provider/
              exc.message.should =~ /libmongocrypt error code/
              exc.message.should_not =~ /CryptError/
            else
              fail 'Expected to raise KmsError'
            end
          end
        end
      end

      context 'with expired server certificate' do
        let(:error_regex) do
          if BSON::Environment.jruby?
            /certificate verify failed/
          else
            /certificate has expired/
          end
        end

        it 'TLS handshake failed' do
          expect do
            client_encryption_expired.create_data_key(
              kms_provider,
              {
                master_key: master_key
            }
            )
          end.to raise_error(Mongo::Error::KmsError, error_regex)
        end
      end

      context 'with server certificate with invalid hostname' do
        let(:error_regex) do
          if BSON::Environment.jruby?
            /TLS handshake failed due to a hostname mismatch/
          else
            /certificate verify failed/
          end
        end

        it 'TLS handshake failed' do
          expect do
            client_encryption_invalid_hostname.create_data_key(
              kms_provider,
              {
                master_key: master_key
              }
            )
          end.to raise_error(Mongo::Error::KmsError, error_regex)
        end
      end
    end

    context 'Azure' do
      let(:kms_provider) do
        'azure'
      end

      let(:master_key) do
        {
          key_vault_endpoint: 'doesnotexist.local',
          key_name: 'foo'
        }
      end

      let(:should_raise_with_tls) do
        true
      end

      it_behaves_like 'it respect KMS TLS options'
    end

    context 'GCP' do
      let(:kms_provider) do
        'gcp'
      end

      let(:master_key) do
        {
          project_id: 'foo',
          location: 'bar',
          key_ring: 'baz',
          key_name: 'foo'
        }
      end

      let(:should_raise_with_tls) do
        true
      end

      it_behaves_like 'it respect KMS TLS options'
    end

    context 'KMIP' do
      let(:kms_provider) do
        'kmip'
      end

      let(:master_key) do
        {}
      end

      let(:should_raise_with_tls) do
        false
      end

      it_behaves_like 'it respect KMS TLS options'
    end

  end
end
