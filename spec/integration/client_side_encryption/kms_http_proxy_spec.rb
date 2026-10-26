# frozen_string_literal: true

require 'spec_helper'

describe 'KMS HTTP proxy' do
  require_libmongocrypt
  require_enterprise

  include_context 'define shared FLE helpers'
  include_context 'with AWS kms_providers'

  # rubocop:disable RSpec/InstanceVariable
  before(:all) do
    unless SpecConfig.instance.fle_aws_key &&
           SpecConfig.instance.fle_aws_secret &&
           SpecConfig.instance.fle_aws_region &&
           SpecConfig.instance.fle_aws_arn

      reason = 'This test requires the MONGO_RUBY_DRIVER_AWS_KEY, ' +
               'MONGO_RUBY_DRIVER_AWS_SECRET, MONGO_RUBY_DRIVER_AWS_REGION, ' +
               'MONGO_RUBY_DRIVER_AWS_ARN environment variables to be set information from AWS.'

      raise(reason) if SpecConfig.instance.fle?

      skip(reason)
    end

    skip 'kms_http_proxy.py is not available' unless KmsHttpProxy.available?

    @http_pid = KmsHttpProxy.start(port: 9004)
    @https_pid = KmsHttpProxy.start(
      port: 9005,
      ca_file: File.expand_path('.evergreen/x509gen/ca.pem'),
      cert_file: File.expand_path('.evergreen/x509gen/server.pem')
    )
  end

  after(:all) do
    KmsHttpProxy.stop(@http_pid) if @http_pid
    KmsHttpProxy.stop(@https_pid) if @https_pid
  end
  # rubocop:enable RSpec/InstanceVariable

  before do
    KmsHttpProxy.reset(port: 9004)
    KmsHttpProxy.reset(port: 9005, tls: true)
  end

  let(:key_vault_client) do
    ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses)
  end

  let(:master_key) do
    {
      region: SpecConfig.instance.fle_aws_region,
      key: SpecConfig.instance.fle_aws_arn,
    }
  end

  let(:data_db) { 'ruby_3813_proxy' }

  def client_encryption(proxy_port:)
    Mongo::ClientEncryption.new(
      key_vault_client,
      kms_tls_options: { aws: default_kms_tls_options_for_provider },
      key_vault_namespace: key_vault_namespace,
      kms_providers: aws_kms_providers,
      kms_connect_callback: KmsHttpProxy.connect_callback(
        proxy_host: '127.0.0.1', proxy_port: proxy_port
      )
    )
  end

  it 'creates a data key through a plain HTTP proxy' do
    ce = client_encryption(proxy_port: 9004)
    expect { ce.create_data_key('aws', master_key: master_key) }.not_to raise_error
    expect(KmsHttpProxy.connect_count(port: 9004)).to be >= 1
  end

  it 'creates a data key through an HTTPS proxy' do
    ce = client_encryption(proxy_port: 9005)
    expect { ce.create_data_key('aws', master_key: master_key) }.not_to raise_error
    expect(KmsHttpProxy.connect_count(port: 9005, tls: true)).to be >= 1
  end

  it 'auto-encrypts and decrypts through the proxy' do
    ce = client_encryption(proxy_port: 9004)
    data_key_id = ce.create_data_key('aws', master_key: master_key)

    schema_map = {
      "#{data_db}.encrypted" => {
        'bsonType' => 'object',
        'properties' => {
          'secret' => {
            'encrypt' => {
              'keyId' => [ data_key_id ],
              'bsonType' => 'string',
              'algorithm' => 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
            }
          }
        }
      }
    }

    encrypted_client = ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      database: data_db,
      auto_encryption_options: {
        key_vault_namespace: key_vault_namespace,
        kms_providers: aws_kms_providers,
        kms_tls_options: { aws: default_kms_tls_options_for_provider },
        schema_map: schema_map,
        kms_connect_callback: KmsHttpProxy.connect_callback(proxy_host: '127.0.0.1', proxy_port: 9004)
      }
    )

    encrypted_client[:encrypted].drop
    encrypted_client[:encrypted].insert_one(secret: 'plaintext')
    expect(encrypted_client[:encrypted].find.first[:secret]).to eq('plaintext')

    unencrypted = key_vault_client.use(data_db)[:encrypted]
    expect(unencrypted.find.first[:secret]).to be_a(BSON::Binary)
    expect(KmsHttpProxy.connect_count(port: 9004)).to be >= 1
  end

  it 'fails when the callback raises' do
    ce = Mongo::ClientEncryption.new(
      key_vault_client,
      kms_tls_options: { aws: default_kms_tls_options_for_provider },
      key_vault_namespace: key_vault_namespace,
      kms_providers: aws_kms_providers,
      kms_connect_callback: proc { |_h, _p, _t| raise IOError, 'no proxy for you' }
    )

    expect do
      ce.create_data_key('aws', master_key: master_key)
    end.to raise_error(Mongo::Error::KmsError)
  end
end
