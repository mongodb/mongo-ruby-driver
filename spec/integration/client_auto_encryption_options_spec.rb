require 'spec_helper'
require 'json'
require 'base64'

describe 'Client auto-encryption options' do
  let(:client) { new_local_client_nmio('mongodb://127.0.0.1:27017/', client_opts) }

  let(:client_opts) { { auto_encryption_opts: auto_encryption_opts } }

  let(:auto_encryption_opts) do
    {
      key_vault_client: key_vault_client,
      key_vault_namespace: key_vault_namespace,
      kms_providers: kms_providers,
      schema_map: schema_map,
      bypass_auto_encryption: bypass_auto_encryption,
      extra_options: extra_options,
    }
  end

  let(:key_vault_client) { new_local_client_nmio('mongodb://127.0.0.1:27018') }
  let(:key_vault_namespace) { 'database.collection' }
  let(:kms_providers) do
    {
      local: kms_local,
      aws: kms_aws,
    }
  end

  let(:kms_local) { { key: Base64.encode64('ruby' * 24) } }
  let(:kms_aws) { { access_key_id: 'ACCESS_KEY_ID', secret_access_key: 'SECRET_ACCESS_KEY' } }

  let(:schema_map) do
    file = File.read('spec/mongo/crypt/data/schema_map.json')
    JSON.parse(file)
  end

  let(:bypass_auto_encryption) { false }

  let(:extra_options) do
    {
      mongocryptd_uri: mongocryptd_uri,
      mongocryptd_bypass_spawn: mongocryptd_bypass_spawn,
      mongocryptd_spawn_path: mongocryptd_spawn_path,
      mongocryptd_spawn_args: mongocryptd_spawn_args,
    }
  end

  let(:mongocryptd_uri) { 'mongodb://localhost:27021' }
  let(:mongocryptd_bypass_spawn) { true }
  let(:mongocryptd_spawn_path) { '/spawn/path' }
  let(:mongocryptd_spawn_args) { ['--idleShutdownTimeoutSecs=100'] }

  context 'when auto_encrypt_opts are nil' do
    let(:auto_encryption_opts) { nil }

    it 'does not raise an exception' do
      expect { client }.not_to raise_error
    end
  end

  context 'when key_vault_namespace is nil' do
    let(:key_vault_namespace) { nil }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /key_vault_namespace option must not be nil/)
    end
  end

  context 'when key_vault_namespace is incorrectly formatted' do
    let(:key_vault_namespace) { 'not.good.formatting' }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /key_vault_namespace must be in the format "database.collection"/)
    end
  end

  context 'when kms_providers is nil' do
    let(:kms_providers) { nil }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /kms_providers option must not be nil/)
    end
  end

  context 'when kms_providers doesn\'t have local or aws keys' do
    let(:kms_providers) { { random_key: 'hello' } }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /kms_providers option must have one of the following keys: :aws, :local/)
    end
  end

  context 'when local kms_provider is incorrectly formatted' do
    let(:kms_providers) { { local: { wrong_key: 'hello' } } }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /kms_providers with :local key must be in the format: { local: { key: 'MASTER-KEY' } }/)
    end
  end

  context 'when aws kms_provider is incorrectly formatted' do
    let(:kms_providers) { { aws: { wrong_key: 'hello' } } }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /kms_providers with :aws key must be in the format: { aws: { access_key_id: 'YOUR-ACCESS-KEY-ID', secret_access_key: 'SECRET-ACCESS-KEY' } }/)
    end
  end

  context 'with all options' do
    it 'does not raise an exception' do
      expect { client }.not_to raise_error

      client_options = client.options[:auto_encryption_opts]

      expect(client_options[:key_vault_client]).to eq(key_vault_client)
      expect(client_options[:key_vault_namespace]).to eq(key_vault_namespace)
      expect(client_options[:kms_providers]).to eq({
        'local' => { 'key' => Base64.encode64('ruby' * 24) },
        'aws' => { 'access_key_id' => 'ACCESS_KEY_ID', 'secret_access_key' => 'SECRET_ACCESS_KEY' }
      })
      expect(client_options[:schema_map]).to eq(schema_map)
      expect(client_options[:bypass_auto_encryption]).to eq(bypass_auto_encryption)
      expect(client_options[:extra_options]).to eq({
        'mongocryptd_uri' => mongocryptd_uri,
        'mongocryptd_bypass_spawn' => mongocryptd_bypass_spawn,
        'mongocryptd_spawn_path' => mongocryptd_spawn_path,
        'mongocryptd_spawn_args' => mongocryptd_spawn_args,
      })
    end
  end

  context 'with default values' do
    let(:auto_encryption_opts) do
      {
        key_vault_namespace: key_vault_namespace,
        kms_providers: kms_providers,
        schema_map: schema_map,
      }
    end

    it 'sets key_vault_client as self' do
      expect(client.options[:auto_encryption_opts][:key_vault_client]).to eq(client)
    end

    it 'sets bypass_auto_encryption to false' do
      expect(client.options[:auto_encryption_opts][:bypass_auto_encryption]).to be false
    end

    it 'sets extra options' do
      expected_extra_options = {
        'mongocryptd_uri' => 'mongodb://localhost:27020',
        'mongocryptd_bypass_spawn' => false,
        'mongocryptd_spawn_path' => '',
        'mongocryptd_spawn_args' => ['--idleShutdownTimeoutSecs=60'],
      }

      expect(client.options[:auto_encryption_opts][:extra_options]).to eq(expected_extra_options)
    end
  end
end
