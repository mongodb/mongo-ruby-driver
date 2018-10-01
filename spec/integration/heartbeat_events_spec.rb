require 'spec_helper'

class TestHeartbeatSubscriber
  def initialize
    @started_events = []
    @succeeded_events = []
    @failed_events = []
  end

  attr_reader :started_events, :succeeded_events, :failed_events

  def started(event)
    @started_events << event
  end

  def succeeded(event)
    @succeeded_events << event
  end

  def failed(event)
    @failed_events << event
  end
end

describe 'Heartbeat events' do
  class HeartbeatEventsSpecTestException < StandardError; end

  let(:subscriber) { TestHeartbeatSubscriber.new }

  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  before do
    Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
  end

  after do
    Mongo::Monitoring::Global.unsubscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
  end

  let(:client) { new_local_client([SpecConfig.instance.addresses.first],
    authorized_client.options.merge(server_selection_timeout: 0.1, connect: :direct)) }

  it 'notifies on successful heartbeats' do
    client.database.command(ismaster: 1)

    started_event = subscriber.started_events.first
    expect(started_event).not_to be nil
    expect(started_event.address).to be_a(Mongo::Address)
    expect(started_event.address.seed).to eq(SpecConfig.instance.addresses.first)

    succeeded_event = subscriber.succeeded_events.first
    expect(succeeded_event).not_to be nil
    expect(succeeded_event.address).to be_a(Mongo::Address)
    expect(succeeded_event.address.seed).to eq(SpecConfig.instance.addresses.first)

    failed_event = subscriber.failed_events.first
    expect(failed_event).to be nil
  end

  it 'notifies on failed heartbeats' do
    exc = HeartbeatEventsSpecTestException.new
    expect_any_instance_of(Mongo::Server::Monitor::Connection).to receive(:ismaster).at_least(:once).and_raise(exc)

    expect do
      client.database.command(ismaster: 1)
    end.to raise_error(Mongo::Error::NoServerAvailable)

    started_event = subscriber.started_events.first
    expect(started_event).not_to be nil
    expect(started_event.address).to be_a(Mongo::Address)
    expect(started_event.address.seed).to eq(SpecConfig.instance.addresses.first)

    succeeded_event = subscriber.succeeded_events.first
    expect(succeeded_event).to be nil

    failed_event = subscriber.failed_events.first
    expect(failed_event).not_to be nil
    expect(failed_event.error).to be exc
    expect(failed_event.failure).to be exc
    expect(failed_event.address).to be_a(Mongo::Address)
    expect(failed_event.address.seed).to eq(SpecConfig.instance.addresses.first)
  end
end
