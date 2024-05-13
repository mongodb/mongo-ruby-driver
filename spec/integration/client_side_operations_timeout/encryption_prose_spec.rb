# frozen_string_literal: true

require 'spec_helper'

describe 'CSOT for encryption' do
  require_libmongocrypt
  require_no_multi_mongos
  min_server_fcv '4.2'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:subscriber) { Mrss::EventSubscriber.new }

  describe 'mongocryptd' do
    before do
      Process.spawn(
        'mongocryptd',
        '--pidfilepath=bypass-spawning-mongocryptd.pid', '--port=23000', '--idleShutdownTimeoutSecs=60',
        %i[ out err ] => '/dev/null'
      )
    end

    let(:client) do
      Mongo::Client.new('mongodb://localhost:23000/?timeoutMS=1000').tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    let(:ping_command) do
      subscriber.started_events.find do |event|
        event.command_name == 'ping'
      end&.command
    end

    after do
      client.close
    end

    it 'does not set maxTimeMS for commands sent to mongocryptd' do
      expect do
        client.use('admin').command(ping: 1)
      end.to raise_error(Mongo::Error::OperationFailure)

      expect(ping_command).not_to have_key('maxTimeMS')
    end
  end

  describe 'ClientEncryption' do
    let(:key_vault_client) do
      ClientRegistry.instance.new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(timeout_ms: 20)
      )
    end

    let(:client_encryption) do
      Mongo::ClientEncryption.new(
        key_vault_client,
        key_vault_namespace: key_vault_namespace,
        kms_providers: local_kms_providers
      )
    end

    describe '#createDataKey' do
      before do
        authorized_client.use(key_vault_db)[key_vault_coll].drop
        authorized_client.use(key_vault_db)[key_vault_coll].create
        authorized_client.use(:admin).command({
                                                configureFailPoint: 'failCommand',
                                                mode: {
                                                  times: 1
                                                },
                                                data: {
                                                  failCommands: [ 'insert' ],
                                                  blockConnection: true,
                                                  blockTimeMS: 30
                                                }
                                              })
      end

      after do
        authorized_client.use(:admin).command({
                                                configureFailPoint: 'failCommand',
                                                mode: 'off',
                                              })
        key_vault_client.close
      end

      it 'fails with timeout error' do
        expect do
          client_encryption.create_data_key('local')
        end.to raise_error(Mongo::Error::TimeoutError)
      end
    end

    describe '#encrypt' do
      let!(:data_key_id) do
        client_encryption.create_data_key('local')
      end

      before do
        authorized_client.use(:admin).command({
                                                configureFailPoint: 'failCommand',
                                                mode: {
                                                  times: 1
                                                },
                                                data: {
                                                  failCommands: [ 'find' ],
                                                  blockConnection: true,
                                                  blockTimeMS: 30
                                                }
                                              })
      end

      after do
        authorized_client.use(:admin).command({
                                                configureFailPoint: 'failCommand',
                                                mode: 'off',
                                              })
      end

      it 'fails with timeout error' do
        expect do
          client_encryption.encrypt('hello', key_id: data_key_id,
                                             algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
        end.to raise_error(Mongo::Error::TimeoutError)
      end
    end
  end
end
