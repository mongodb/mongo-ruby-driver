require 'spec_helper'

describe 'Auto Encryption' do
  require_libmongocrypt
  min_server_fcv '4.2'
  require_enterprise

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  context 'with an invalid mongocryptd spawn path' do
    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: kms_providers,
            key_vault_namespace: key_vault_namespace,
            schema_map: { 'auto_encryption.users' => schema_map },
            extra_options: {
              mongocryptd_spawn_path: 'echo hello world',
              mongocryptd_spawn_args: []
            }
          },
          database: 'auto_encryption'
        ),
      )
    end

    before do
      authorized_client.use(:admin)[:datakeys].drop
      authorized_client.use(:admin)[:datakeys].insert_one(data_key)
    end

    it 'raises an exception when trying to perform auto encryption' do
      expect do
        client[:users].insert_one(ssn: ssn)
      end.to raise_error(
        Mongo::Error::MongocryptdSpawnError,
        /Failed to spawn mongocryptd at the path "echo hello world" with arguments/
      )
    end
  end
end
