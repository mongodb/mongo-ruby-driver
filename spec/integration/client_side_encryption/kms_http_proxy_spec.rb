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

    # The proxy ports are fixed, so a proxy leaked by a crashed run would
    # otherwise make this group fail with a confusing "did not become ready"
    # error, or silently drive the stale listener. Fail early instead.
    assert_port_free(9004)
    assert_port_free(9005)

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

  def client_encryption(proxy_port:, tls: false)
    Mongo::ClientEncryption.new(
      key_vault_client,
      key_vault_namespace: key_vault_namespace,
      kms_providers: aws_kms_providers,
      kms_connect_callback: KmsHttpProxy.connect_callback(
        proxy_host: '127.0.0.1', proxy_port: proxy_port, tls: tls
      )
    )
  end

  # Any failure to connect means nothing is listening yet, which is what we
  # want; only a successful connection means the port is taken.
  def assert_port_free(port)
    TCPSocket.new('127.0.0.1', port).close
    raise "port #{port} is already in use; a leaked kms_http_proxy from an " \
          'earlier run may still be listening. Kill it and re-run.'
  rescue SystemCallError
    nil
  end

  it 'creates a data key through a plain HTTP proxy' do
    ce = client_encryption(proxy_port: 9004)
    expect { ce.create_data_key('aws', master_key: master_key) }.not_to raise_error
    expect(KmsHttpProxy.connect_count(port: 9004)).to be >= 1
  end

  it 'creates a data key through an HTTPS proxy' do
    ce = client_encryption(proxy_port: 9005, tls: true)
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
        schema_map: schema_map,
        extra_options: extra_options,
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
      key_vault_namespace: key_vault_namespace,
      kms_providers: aws_kms_providers,
      kms_connect_callback: proc { |_h, _p, _t| raise IOError, 'no proxy for you' }
    )

    expect do
      ce.create_data_key('aws', master_key: master_key)
    end.to raise_error(Mongo::Error::KmsError)
  end

  it 'passes a non-zero timeout to the callback when timeoutMS is set' do
    received = nil
    callback = lambda do |host, port, timeout_ms|
      received = timeout_ms
      KmsHttpProxy.connect_callback(proxy_host: '127.0.0.1', proxy_port: 9004)
                  .call(host, port, timeout_ms)
    end

    ce = Mongo::ClientEncryption.new(
      key_vault_client,
      # Deliberate deviation from the upstream spec (section 28 case 5 uses
      # timeoutMS: 1000): a longer timeout avoids flakes against real AWS. The
      # requirement exercised here is only that a non-zero Integer reaches the
      # callback.
      timeout_ms: 30_000,
      key_vault_namespace: key_vault_namespace,
      kms_providers: aws_kms_providers,
      kms_connect_callback: callback
    )

    ce.create_data_key('aws', master_key: master_key)
    expect(received).to be_a(Integer)
    expect(received).to be > 0
  end

  it 'retries the callback after a transient failure' do
    attempts = 0
    proxy = KmsHttpProxy.connect_callback(proxy_host: '127.0.0.1', proxy_port: 9004)
    callback = lambda do |host, port, timeout_ms|
      attempts += 1
      raise IOError, 'simulated transient failure' if attempts == 1

      proxy.call(host, port, timeout_ms)
    end

    ce = Mongo::ClientEncryption.new(
      key_vault_client,
      key_vault_namespace: key_vault_namespace,
      kms_providers: aws_kms_providers,
      kms_connect_callback: callback
    )

    expect { ce.create_data_key('aws', master_key: master_key) }.not_to raise_error
    expect(attempts).to be >= 2
  end
end
