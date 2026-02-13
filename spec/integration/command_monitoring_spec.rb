# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Command monitoring' do

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.with(app_name: 'command monitoring spec').tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  it 'notifies on successful commands' do
    result = client.database.command(hello: 1)
    expect(result.documents.first['isWritablePrimary']).to be true

    started_events = subscriber.started_events.select do |event|
      event.command_name == 'hello'
    end
    expect(started_events.length).to eql(1)
    started_event = started_events.first
    expect(started_event.command_name).to eql('hello')
    expect(started_event.address).to be_a(Mongo::Address)
    expect(started_event.command).to have_key('$db')

    succeeded_events = subscriber.succeeded_events.select do |event|
      event.command_name == 'hello'
    end
    expect(succeeded_events.length).to eql(1)
    succeeded_event = succeeded_events.first
    expect(succeeded_event.command_name).to eql('hello')
    expect(succeeded_event.reply).to be_a(BSON::Document)
    expect(succeeded_event.reply['isWritablePrimary']).to eql(true)
    expect(succeeded_event.reply['ok']).to eq(1)
    expect(succeeded_event.address).to be_a(Mongo::Address)
    expect(succeeded_event.duration).to be_a(Float)

    expect(subscriber.failed_events.length).to eql(0)
  end

  it 'notifies on failed commands' do
    expect do
      result = client.database.command(:bogus => 1)
    end.to raise_error(Mongo::Error::OperationFailure, /no such c(om)?m(an)?d/)

    started_events = subscriber.started_events.select do |event|
      event.command_name == 'bogus'
    end
    expect(started_events.length).to eql(1)
    started_event = started_events.first
    expect(started_event.command_name).to eql('bogus')
    expect(started_event.address).to be_a(Mongo::Address)

    succeeded_events = subscriber.succeeded_events.select do |event|
      event.command_name == 'hello'
    end
    expect(succeeded_events.length).to eql(0)

    failed_events = subscriber.failed_events.select do |event|
      event.command_name == 'bogus'
    end
    expect(failed_events.length).to eql(1)
    failed_event = failed_events.first
    expect(failed_event.command_name).to eql('bogus')
    expect(failed_event.message).to match(/no such c(om)?m(an)?d/)
    expect(failed_event.address).to be_a(Mongo::Address)
    expect(failed_event.duration).to be_a(Float)
  end

  context 'client with no established connections' do
    # X.509 auth uses authenticate instead of sasl* commands
    require_no_external_user

    shared_examples_for 'does not nest auth and find' do
      it 'does not nest auth and find' do
        expect(subscriber.started_events.length).to eq 0
        client['test-collection'].find(a: 1).first
        command_names = subscriber.started_events.map(&:command_name)
        command_names.should == expected_command_names
      end
    end

    context 'pre-4.4 servers' do
      max_server_version '4.2'

      let(:expected_command_names) do
        # Long SCRAM conversation.
        %w(saslStart saslContinue saslContinue find)
      end

      it_behaves_like 'does not nest auth and find'
    end

    context '4.4+ servers' do
      min_server_fcv '4.4'

      let(:expected_command_names) do
        # Speculative auth + short SCRAM conversation.
        %w(saslContinue find)
      end

      it_behaves_like 'does not nest auth and find'
    end
  end

  context 'when write concern is specified outside of command document' do
    require_topology :replica_set

    let(:collection) do
      client['command-monitoring-test']
    end
    let(:write_concern) { Mongo::WriteConcern.get({w: 42}) }
    let(:session) { client.start_session }
    let(:command) do
      Mongo::Operation::Command.new(
        selector: { commitTransaction: 1 },
        db_name: 'admin',
        session: session,
        txn_num: 123,
        write_concern: write_concern,
      )
    end

    it 'includes write concern in notified command document' do
      server = client.cluster.next_primary
      collection.insert_one(a: 1)
      session.start_transaction
      collection.insert_one({a: 1}, session: session)

      subscriber.clear_events!
      expect do
        command.execute(server, context: Mongo::Operation::Context.new(session: session))
      end.to raise_error(Mongo::Error::OperationFailure, /100\b.*Not enough data-bearing nodes/)

      expect(subscriber.started_events.length).to eq(1)
      event = subscriber.started_events.first
      expect(event.command['writeConcern']['w']).to eq(42)
    end
  end
end
