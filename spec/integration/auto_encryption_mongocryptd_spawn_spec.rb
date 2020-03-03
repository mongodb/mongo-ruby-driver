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

    let(:server_selector) { double("ServerSelector") }
    let(:cluster) { double("Cluster") }

    before do
      authorized_client.use(:admin)[:datakeys].drop
      authorized_client.use(:admin)[:datakeys].insert_one(data_key)

      allow(server_selector).to receive(:name)
      allow(server_selector).to receive(:server_selection_timeout)
      allow(server_selector).to receive(:local_threshold)
      allow(cluster).to receive(:summary)

      # Raise a server selection error on intent-to-encrypt commands to mock
      # what would happen if mongocryptd hadn't already been spawned. It is
      # necessary to mock this behavior because it is likely that another test
      # will have already spawned mongocryptd, causing this test to fail.
      allow_any_instance_of(Mongo::Database)
        .to receive(:command)
        .with(
          hash_including(
            'insert' => 'users',
            '$db' => 'auto_encryption',
            'ordered' => true,
            'lsid' => kind_of(Hash),
            'documents' => kind_of(Array),
            'jsonSchema' => kind_of(Hash),
            'isRemoteSchema' => false,
          )
        )
        .and_raise(Mongo::Error::NoServerAvailable.new(server_selector, cluster))
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

  context 'prose tests' do
    context 'via mongocryptdBypassSpawn' do
      let(:client) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: kms_providers,
              key_vault_namespace: 'admin.datakeys',
              schema_map: { 'db.coll' => schema_map },
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
              kms_providers: kms_providers,
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
