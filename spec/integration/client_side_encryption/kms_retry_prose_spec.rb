# frozen_string_literal: true

require 'spec_helper'

def simulate_failure(type, times = 1)
  # Define the URL and the data
  url = URI.parse("https://localhost:9003/set_failpoint/#{type}")
  data = { count: times }.to_json
  # Create a new HTTP object
  http = Net::HTTP.new(url.host, url.port)
  # Enable SSL/TLS
  http.use_ssl = true
  http.verify_mode = OpenSSL::SSL::VERIFY_NONE
  # Load the CA certificate
  http.ca_file = '.evergreen/x509gen/ca.pem'
  # Create a new POST request
  request = Net::HTTP::Post.new(url.path, { 'Content-Type' => 'application/json' })
  request.body = data
  # Execute the request
  http.request(request)
end

describe 'KMS Retry Prose Spec' do
  require_libmongocrypt
  require_enterprise
  min_server_version '4.2'

  include_context 'define shared FLE helpers'

  let(:key_vault_client) do
    ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses)
  end

  let(:client_encryption) do
    Mongo::ClientEncryption.new(
      key_vault_client,
      kms_tls_options: {
        aws: default_kms_tls_options_for_provider,
        gcp: default_kms_tls_options_for_provider,
        azure: default_kms_tls_options_for_provider,
      },
      key_vault_namespace: key_vault_namespace,
      # For some reason libmongocrypt ignores custom endpoints for Azure and CGP
      # kms_providers: aws_kms_providers.merge(azure_kms_providers).merge(gcp_kms_providers)
      kms_providers: aws_kms_providers
    )
  end

  shared_examples 'kms_retry prose spec' do
    it 'createDataKey and encrypt with TCP retry' do
      simulate_failure('network')
      data_key_id = client_encryption.create_data_key(kms_provider, master_key: master_key)
      simulate_failure('network')
      expect {
        client_encryption.encrypt(123, key_id: data_key_id, algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
      }.not_to raise_error
    end

    it 'createDataKey and encrypt with HTTP retry' do
      simulate_failure('http')
      data_key_id = client_encryption.create_data_key(kms_provider, master_key: master_key)
      simulate_failure('http')
      expect {
        client_encryption.encrypt(123, key_id: data_key_id, algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
      }.not_to raise_error
    end

    it 'createDataKey fails after too many retries' do
      simulate_failure('network', 4)
      expect {
        client_encryption.create_data_key(kms_provider, master_key: master_key)
      }.to raise_error(Mongo::Error::KmsError)
    end
  end

  context 'aws' do
    let(:kms_provider) { 'aws' }

    let(:master_key) do
      {
        region: "foo",
        key: "bar",
        endpoint: "127.0.0.1:9003",
      }
    end

    include_examples 'kms_retry prose spec'
  end

  # For some reason libmongocrypt ignores custom endpoints for Azure and CGP
  xcontext 'gcp' do
    let(:kms_provider) { 'gcp' }

    let(:master_key) do
      {
        project_id: "foo",
        location: "bar",
        key_ring: "baz",
        key_name: "qux",
        endpoint: "127.0.0.1:9003"
      }
    end

    include_examples 'kms_retry prose spec'
  end

  # For some reason libmongocrypt ignores custom endpoints for Azure and CGP
  xcontext 'azure' do
    let(:kms_provider) { 'azure' }

    let(:master_key) do
      {
        key_vault_endpoint: "127.0.0.1:9003",
        key_name: "foo",
      }
    end

    include_examples 'kms_retry prose spec'
  end
end
