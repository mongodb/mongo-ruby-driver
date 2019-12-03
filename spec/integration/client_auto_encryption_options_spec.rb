require 'spec_helper'
require 'json'
require 'base64'

describe 'Client auto-encryption options' do
  describe 'Spawning mongocryptd' do
    require_enterprise

    let(:client) do
      ClientRegistry.instance.new_local_client(
        [SpecConfig.instance.addresses.first],
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
  end
end
