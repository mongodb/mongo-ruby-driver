require 'spec_helper'
require 'json'
require 'base64'

describe 'Client auto-encryption options' do
  require_libmongocrypt
  min_server_version '4.2'

  let(:client) { new_local_client_nmio('mongodb://127.0.0.1:27017/', client_opts) }

  let(:client_opts) { { auto_encryption_options: auto_encryption_options } }

  let(:auto_encryption_options) do
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
    JSON.parse(File.read('spec/mongo/crypt/data/schema_map.json'))
  end

  let(:bypass_auto_encryption) { true }

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
    let(:auto_encryption_options) { nil }

    it 'does not raise an exception' do
      expect { client }.not_to raise_error
    end

    it 'does not spawn a mongocryptd process' do
      expect(client).not_to receive(:spawn_mongocryptd)
    end
  end

  context 'when key_vault_namespace is nil' do
    let(:key_vault_namespace) { nil }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /key_vault_namespace option cannot be nil/)
    end
  end

  context 'when key_vault_namespace is incorrectly formatted' do
    let(:key_vault_namespace) { 'not.good.formatting' }

    it 'raises an exception' do
      expect { client }.to raise_error(ArgumentError, /key_vault_namespace option must be in the format database.collection/)
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

  context 'with valid options' do
    it 'does not raise an exception' do
      expect { client }.not_to raise_error
    end

    it 'sets options on the client' do
      client_options = client.encryption_options

      expect(client_options[:key_vault_client]).to eq(key_vault_client)
      expect(client_options[:key_vault_namespace]).to eq(key_vault_namespace)
      expect(client_options[:kms_providers]).to eq({
        'local' => { 'key' => Base64.encode64('ruby' * 24) },
        'aws' => { 'access_key_id' => 'ACCESS_KEY_ID', 'secret_access_key' => 'SECRET_ACCESS_KEY' }
      })
      expect(client_options[:schema_map]).to eq(schema_map)
      expect(client_options[:bypass_auto_encryption]).to eq(bypass_auto_encryption)
      expect(client_options[:mongocryptd_uri]).to eq(mongocryptd_uri)
      expect(client_options[:mongocryptd_bypass_spawn]).to eq(mongocryptd_bypass_spawn)
      expect(client_options[:mongocryptd_spawn_path]).to eq(mongocryptd_spawn_path)
      expect(client_options[:mongocryptd_spawn_args]).to eq(mongocryptd_spawn_args)

      expect(client.mongocryptd_client.options[:monitoring_io]).to be false
    end

    context 'with default extra options' do
      let(:auto_encryption_options) do
        {
          key_vault_namespace: key_vault_namespace,
          kms_providers: kms_providers,
          schema_map: schema_map,
        }
      end

      it 'sets key_vault_client as self' do
        expect(client.encryption_options[:key_vault_client]).to eq(client)
      end

      it 'sets bypass_auto_encryption to false' do
        expect(client.encryption_options[:bypass_auto_encryption]).to be false
      end

      it 'sets extra options to defaults' do
        client_options = client.encryption_options

        expect(client_options[:mongocryptd_uri]).to eq('mongodb://localhost:27020')
        expect(client_options[:mongocryptd_bypass_spawn]).to be false
        expect(client_options[:mongocryptd_spawn_path]).to eq('mongocryptd')
        expect(client_options[:mongocryptd_spawn_args]).to eq(['--idleShutdownTimeoutSecs=60'])
      end
    end
  end

  describe 'Spawning mongocryptd' do
    require_enterprise

    let(:client) do
      ClientRegistry.instance.new_local_client(
        'mongodb://127.0.0.1:27017',
        {
          auto_encryption_options: {
            key_vault_client: new_local_client_nmio('mongodb://127.0.0.1:27018'),
            key_vault_namespace: 'database.collection',
            kms_providers: {
              local: { key: Base64.encode64('ruby' * 24) },
            },
            extra_options: extra_options
          }
        }
      )
    end

    let(:extra_options) { {} }

    describe '#initialize' do
      it 'spawns mongocryptd' do
        pid = client.mongocryptd_pid

        # Verify that the process at pid is still running -
        # every active process will have a process group, so
        # if the process group id is a number, the process is
        # still running
        expect(Process.getpgid(pid)).to be_a_kind_of(Numeric)
      end

      context 'with mongocryptd_bypass_spawn: true' do
        let(:extra_options) do
          {
            mongocryptd_bypass_spawn: true,
          }
        end

        it 'does not spawn mongocryptd' do
          expect_any_instance_of(Mongo::Client).not_to receive(:spawn_mongocryptd)
          expect(client.mongocryptd_pid).to be_nil
        end
      end

      context 'with empty arguments and shell command as path' do
        let(:extra_options) do
          {
            mongocryptd_spawn_path: 'echo hello world',
            mongocryptd_spawn_args: []
          }
        end

        it 'attempmts to spawn mongocryptd at path and throws an error' do
          expect do
            client
          end.to raise_error(Errno::ENOENT, /No such file or directory - echo hello world/)
        end
      end
    end

    describe '#close' do
      it 'kills mongocryptd process' do
        pid = client.mongocryptd_pid

        client.close
        expect(client.mongocryptd_pid).to be_nil

        # Verify that there is no process running at pid -
        # the getpgid method will throw and error if
        # no process is running at that pid
        expect do
          Process.getpgid(pid)
        end.to raise_error(Errno::ESRCH, /No such process/)
      end

      context 'if mongocryptd process has already been killed' do
        before do
          pid = client.mongocryptd_pid

          Process.kill('ABRT', pid)
          Process.wait(pid)
        end

        it 'does not throw an error' do
          expect do
            client.close
          end.not_to raise_error
        end
      end
    end
  end
end
