require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: Bypass mongocryptd spawn' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    context 'via mongocryptdBypassSpawn' do
      let(:test_schema_map) do
        BSON::ExtJSON.parse(File.read('spec/support/crypt/external/external-schema.json'))
      end

      let(:client) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: local_kms_providers,
              key_vault_namespace: 'admin.datakeys',
              schema_map: { 'db.coll' => test_schema_map },
              extra_options: {
                mongocryptd_bypass_spawn: true,
                mongocryptd_uri: "mongodb://localhost:27090/db?serverSelectionTimeoutMS=1000",
                mongocryptd_spawn_args: [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27090"],
              },
            },
            database: :db
          ),
        )
      end

      it 'does not spawn' do
        lambda do
          client[:coll].insert_one(encrypted: 'test')
        end.should raise_error(Mongo::Error::NoServerAvailable, /Server address=localhost:27090 UNKNOWN/)
      end
    end

    context 'via bypassAutoEncryption' do
      let(:client) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: local_kms_providers,
              key_vault_namespace: 'admin.datakeys',
              bypass_auto_encryption: true,
              extra_options: {
                mongocryptd_spawn_args: [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27090"],
              },
            },
            database: :db
          ),
        )
      end

      let(:mongocryptd_client) do
        new_local_client(['localhost:27090'], server_selection_timeout: 1)
      end

      it 'does not spawn' do
        lambda do
          client[:coll].insert_one(encrypted: 'test')
        end.should_not raise_error
        lambda do
          mongocryptd_client.database.command(ismaster: 1)
        end.should raise_error(Mongo::Error::NoServerAvailable)
      end
    end
  end
end
