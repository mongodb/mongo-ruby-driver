# frozen_string_literal: true

require 'spec_helper'

describe 'Decryption events' do
  require_enterprise
  min_server_fcv '4.2'
  require_libmongocrypt
  include_context 'define shared FLE helpers'
  require_topology :replica_set
  min_server_version '7.0.0-rc0'

  let(:setup_client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        database: SpecConfig.instance.test_db
      )
    )
  end

  let(:collection_name) do
    'decryption_event'
  end

  let(:client_encryption) do
    Mongo::ClientEncryption.new(
      setup_client,
      key_vault_namespace: "#{key_vault_db}.#{key_vault_coll}",
      kms_providers: local_kms_providers
    )
  end

  let(:key_id) do
    client_encryption.create_data_key('local')
  end

  let(:unencrypted_value) do
    'hello'
  end

  let(:ciphertext) do
    client_encryption.encrypt(
      unencrypted_value,
      key_id: key_id,
      algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
    )
  end

  let(:malformed_ciphertext) do
    ciphertext.dup.tap do |obj|
      obj.data[-1] = 0.chr
    end
  end

  let(:encrypted_client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          key_vault_namespace: "#{key_vault_db}.#{key_vault_coll}",
          kms_providers: local_kms_providers,
          extra_options: extra_options,
        },
        database: SpecConfig.instance.test_db,
        retry_reads: false,
        max_read_retries: 0
      )
    )
  end

  let(:collection) do
    encrypted_client[collection_name]
  end

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:command_error) do
    {
      'configureFailPoint' => 'failCommand',
      'mode' => { 'times' => 1 },
      'data' => {
        'errorCode' => 123,
        'failCommands' => [ 'aggregate' ]
      }
    }
  end

  let(:network_error) do
    {
      'configureFailPoint' => 'failCommand',
      'mode' => { 'times' => 1 },
      'data' => {
        'errorCode' => 123,
        'closeConnection' => true,
        'failCommands' => [ 'aggregate' ]
      }
    }
  end

  let(:aggregate_event) do
    subscriber.succeeded_events.detect do |evt|
      evt.command_name == 'aggregate'
    end
  end

  before do
    setup_client[collection_name].drop
    setup_client[collection_name].create

    encrypted_client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
  end

  it 'tests command error' do
    setup_client.use(:admin).command(command_error)

    expect do
      collection.aggregate([]).to_a
    end.to raise_error(Mongo::Error::OperationFailure, /Failing command (?:via|due to) 'failCommand' failpoint/)
    expect(subscriber.failed_events.length).to be 1
  end

  it 'tests network error' do
    setup_client.use(:admin).command(network_error)

    expect do
      collection.aggregate([]).to_a
    end.to raise_error(Mongo::Error::SocketError)
    expect(subscriber.failed_events.length).to be 1
  end

  context 'when decrypt error' do
    before do
      collection.insert_one(encrypted: malformed_ciphertext)
    end

    it 'fails' do
      expect { collection.aggregate([]).to_a }.to raise_error(Mongo::Error::CryptError)
      expect(aggregate_event).not_to be_nil
      expect(
        aggregate_event.reply.dig('cursor', 'firstBatch')&.first&.dig('encrypted')
      ).to be_a(BSON::Binary)
    end
  end

  context 'when decrypt success' do
    before do
      collection.insert_one(encrypted: ciphertext)
    end

    it 'succeeds' do
      expect { collection.aggregate([]).to_a }.not_to raise_error
      expect(aggregate_event).not_to be_nil
      expect(
        aggregate_event.reply.dig('cursor', 'firstBatch')&.first&.dig('encrypted')
      ).to be_a(BSON::Binary)
    end
  end
end
