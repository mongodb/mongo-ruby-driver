# frozen_string_literal: true
# rubocop:todo all

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
      key_vault_collection.drop
      key_vault_collection.insert_one(data_key)

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
            'ordered' => true,
            'lsid' => kind_of(Hash),
            'documents' => kind_of(Array),
            'jsonSchema' => kind_of(Hash),
            'isRemoteSchema' => false,
          ),
          { execution_options: { deserialize_as_bson: true } },
        )
        .and_raise(Mongo::Error::NoServerAvailable.new(server_selector, cluster))
    end

    it 'raises an exception when trying to perform auto encryption' do
      expect do
        client['users'].insert_one(ssn: ssn)
      end.to raise_error(
        Mongo::Error::MongocryptdSpawnError,
        /Failed to spawn mongocryptd at the path "echo hello world" with arguments/
      )
    end
  end
end
