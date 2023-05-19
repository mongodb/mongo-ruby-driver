# frozen_string_literal: true

require 'spec_helper'

describe 'mongocryptd prose tests' do
  require_libmongocrypt
  require_enterprise
  min_server_version '7.0.0-rc0'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:mongocryptd_uri) { 'mongodb://localhost:27777' }

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          kms_tls_options: kms_tls_options,
          key_vault_namespace: key_vault_namespace,
          schema_map: { 'auto_encryption.users' => schema_map },
          extra_options: extra_options,
        },
        database: 'auto_encryption'
      )
    )
  end

  before do
    skip 'This test requires crypt shared library' unless SpecConfig.instance.crypt_shared_lib_path

    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)

    encryption_client['users'].drop
  end

  context 'when shared library is loaded' do
    let(:extra_options) do
      {
        crypt_shared_lib_path: SpecConfig.instance.crypt_shared_lib_path,
        mongocryptd_uri: mongocryptd_uri
      }
    end

    let!(:connect_attempt) do
      Class.new do
        def lock
          @lock ||= Mutex.new
        end

        def done?
          lock.synchronize do
            !!@done
          end
        end

        def done!
          lock.synchronize do
            @done = true
          end
        end
      end.new
    end

    let!(:listener) do
      Thread.new do
        TCPServer.new(27_777).accept
        connect_attempt.done!
      end
    end

    after do
      listener.exit
    end

    it 'does not try to connect to mongocryptd' do
      encryption_client[:users].insert_one(ssn: ssn)
      expect(connect_attempt.done?).to be false
    end
  end

  context 'when shared library is required' do
    let(:extra_options) do
      {
        crypt_shared_lib_path: SpecConfig.instance.crypt_shared_lib_path,
        crypt_shared_lib_required: true,
        mongocryptd_uri: mongocryptd_uri,
        mongocryptd_spawn_args: [ '--pidfilepath=bypass-spawning-mongocryptd.pid', '--port=27777' ]
      }
    end

    let(:mongocryptd_client) { new_local_client(mongocryptd_uri) }

    it 'does not spawn mongocryptd' do
      expect { encryption_client[:users].insert_one(ssn: ssn) }
        .not_to raise_error

      expect { mongocryptd_client.database.command(hello: 1) }
        .to raise_error(Mongo::Error::NoServerAvailable)
    end
  end
end
