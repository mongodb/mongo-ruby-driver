# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'SDAM events' do
  let(:subscriber) { Mrss::EventSubscriber.new }

  describe 'server closed event' do
    it 'is published when client is closed' do
      client = ClientRegistry.instance.new_local_client(
        SpecConfig.instance.addresses, SpecConfig.instance.test_options)
      client.subscribe(Mongo::Monitoring::SERVER_CLOSED, subscriber)

      # get the client connected
      client.database.command(ping: 1)
      expect(subscriber.succeeded_events).to be_empty

      client.close

      expect(subscriber.succeeded_events).not_to be_empty
      event = subscriber.first_event('server_closed_event')
      expect(event).not_to be_nil
    end
  end

  describe 'topology closed event' do
    it 'is published when client is closed' do
      client = ClientRegistry.instance.new_local_client(
        SpecConfig.instance.addresses, SpecConfig.instance.test_options)
      client.subscribe(Mongo::Monitoring::TOPOLOGY_CLOSED, subscriber)

      # get the client connected
      client.database.command(ping: 1)
      expect(subscriber.succeeded_events).to be_empty

      client.close

      expect(subscriber.succeeded_events).not_to be_empty
      event = subscriber.first_event('topology_closed_event')
      expect(event).not_to be_nil

      expect(event.topology).to eql(client.cluster.topology)
    end
  end

  describe 'heartbeat event' do
    require_topology :single

    context 'pre-4.4 servers' do
      max_server_version '4.2'

      let(:sdam_proc) do
        Proc.new do |client|
          client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
        end
      end

      let(:client) do
        new_local_client(SpecConfig.instance.addresses,
          # Heartbeat interval is bound by 500 ms
          SpecConfig.instance.test_options.merge(
            heartbeat_frequency: 0.5,
            sdam_proc: sdam_proc
          ),
        )
      end

      it 'is published every heartbeat interval' do
        client
        sleep 4
        client.close

        started_events = subscriber.select_started_events(Mongo::Monitoring::Event::ServerHeartbeatStarted)
        # Expect about 8 events, maybe 9 or 7
        expect(started_events.length).to be >= 6
        expect(started_events.length).to be <= 10

        succeeded_events = subscriber.select_succeeded_events(Mongo::Monitoring::Event::ServerHeartbeatSucceeded)
        expect(started_events.length).to be > 1
        expect(succeeded_events.length..succeeded_events.length+1).to include(started_events.length)
      end
    end

    context '4.4+ servers' do
      min_server_fcv '4.4'

      let(:sdam_proc) do
        Proc.new do |client|
          client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
        end
      end

      let(:client) do
        new_local_client(SpecConfig.instance.addresses,
          # Heartbeat interval is bound by 500 ms
          SpecConfig.instance.test_options.merge(
            heartbeat_frequency: 0.5,
            sdam_proc: sdam_proc
          ),
        )
      end

      it 'is published up to twice every heartbeat interval' do
        client
        sleep 3
        client.close

        started_events = subscriber.select_started_events(
          Mongo::Monitoring::Event::ServerHeartbeatStarted
        )
        # We could have up to 16 events and should have no fewer than 8 events.
        # Whenever an awaited hello succeeds while the regular monitor is
        # waiting, the regular monitor's next scan is pushed forward.
        expect(started_events.length).to be >= 6
        expect(started_events.length).to be <= 18
        expect(started_awaited = started_events.select(&:awaited?)).not_to be_empty
        expect(started_regular = started_events.reject(&:awaited?)).not_to be_empty

        completed_events = subscriber.select_completed_events(
          Mongo::Monitoring::Event::ServerHeartbeatSucceeded,
          Mongo::Monitoring::Event::ServerHeartbeatFailed,
        )
        expect(completed_events.length).to be >= 6
        expect(completed_events.length).to be <= 18
        expect(succeeded_awaited = completed_events.select(&:awaited?)).not_to be_empty
        expect(succeeded_regular = completed_events.reject(&:awaited?)).not_to be_empty

        # There may be in-flight hellos that don't complete, both
        # regular and awaited.
        expect(started_awaited.length).to be > 1
        expect(succeeded_awaited.length..succeeded_awaited.length+1).to include(started_awaited.length)
        expect(started_regular.length).to be > 1
        expect(succeeded_regular.length..succeeded_regular.length+1).to include(started_regular.length)
      end
    end
  end

  describe 'server description changed' do
    require_topology :single

    let(:sdam_proc) do
      Proc.new do |client|
        client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, subscriber)
      end
    end

    let(:client) do
      new_local_client(SpecConfig.instance.addresses,
        # Heartbeat interval is bound by 500 ms
        SpecConfig.instance.test_options.merge(client_options).merge(
          heartbeat_frequency: 0.5,
          sdam_proc: sdam_proc,
        ),
      )
    end

    let(:client_options) do
      {}
    end

    it 'is not published when there are no changes in server state' do
      client
      sleep 6
      client.close

      events = subscriber.select_succeeded_events(Mongo::Monitoring::Event::ServerDescriptionChanged)

      # In 6 seconds we should have about 10 or 12 heartbeats.
      # We expect 1 or 2 description changes:
      # The first one from unknown to known,
      # The second one because server changes the fields it returns based on
      # driver server check payload (e.g. ismaster/isWritablePrimary).
      expect(events.length).to be >= 1
      expect(events.length).to be <= 2
    end
  end
end
