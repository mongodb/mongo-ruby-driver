require 'spec_helper'

describe 'Auto Encryption' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:subscriber) { EventSubscriber.new }

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          schema_map: { "auto_encryption.users" => schema_map },
          # bypass_auto_encryption: bypass_auto_encryption
        },
        database: 'auto_encryption'
      ),
    ).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  before(:each) do
    admin_client = authorized_client.use(:admin)
    admin_client[:datakeys].drop
    admin_client[:datakeys].insert_one(data_key)

    encryption_client[:users].drop
    result = encryption_client[:users].insert_one(ssn: ssn)
  end

  let(:events) do
    events = []
    events << subscriber.started_events.select do |event|
      event.command_name == 'insert'
    end
  end

  it 'has encrypted data in command monitoring' do
    result = encryption_client[:users].find(ssn: ssn).first

    started_event = subscriber.started_events.find do |event|
      event.command_name == 'find'
    end

    succeeded_event = subscriber.succeeded_events.find do |event|
      event.command_name == 'find'
    end

    # Command started event occurs after ssn is encrypted
    expect(started_event.command["filter"]["ssn"]["$eq"]).to be_ciphertext

    # Command succeeded event occurs before ssn is decrypted
    expect(succeeded_event.reply["cursor"]["firstBatch"].first["ssn"]).to be_ciphertext
  end
end
