require 'spec_helper'

describe 'SDAM events' do
  let(:subscriber) { EventSubscriber.new }

  describe 'server closed event' do
    it 'is published when client is closed' do
      client = ClientRegistry.instance.new_local_client(
        SpecConfig.instance.addresses, SpecConfig.instance.test_options)
      client.subscribe(Mongo::Monitoring::SERVER_CLOSED, subscriber)

      # get the client connected
      client.database.command(ismaster: 1)
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
      client.database.command(ismaster: 1)
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

      let(:client) do
        new_local_client(SpecConfig.instance.addresses,
          # Heartbeat interval is bound by 500 ms
          SpecConfig.instance.test_options.merge(heartbeat_frequency: 0.5),
        ).tap do |client|
          client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
        end
      end

      it 'is published every heartbeat interval' do
        client
        sleep 4
        client.close

        started_events = subscriber.select_started_events(Mongo::Monitoring::Event::ServerHeartbeatStarted)
        # Expect about 8 events, maybe 9 or 7
        started_events.length.should >= 6
        started_events.length.should <= 10

        succeeded_events = subscriber.select_succeeded_events(Mongo::Monitoring::Event::ServerHeartbeatSucceeded)
        # Since we gracefully close the client, we expect each heartbeat
        # to complete.
        started_events.length.should == succeeded_events.length
      end
    end

    context '4.4+ servers' do
      min_server_fcv '4.4'

      let(:client) do
        new_local_client(SpecConfig.instance.addresses,
          # Heartbeat interval is bound by 500 ms
          SpecConfig.instance.test_options.merge(heartbeat_frequency: 0.5),
        ).tap do |client|
          client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
        end
      end

      it 'is published up to twice every heartbeat interval' do
        client
        sleep 3
        client.close

        events = subscriber.select_started_events(Mongo::Monitoring::Event::ServerHeartbeatStarted)
        # We could have up to 16 events and should have no fewer than 8 events.
        # Whenever an awaited ismaster succeeds while the regular monitor is
        # waiting, the regular monitor's next scan is pushed forward.
        events.length.should >= 6
        events.length.should <= 18
        (started_awaited = events.select(&:awaited?)).should_not be_empty
        (started_regular = events.reject(&:awaited?)).should_not be_empty

        events = subscriber.select_succeeded_events(Mongo::Monitoring::Event::ServerHeartbeatSucceeded)
        events.length.should >= 6
        events.length.should <= 18
        (succeeded_awaited = events.select(&:awaited?)).should_not be_empty
        (succeeded_regular = events.reject(&:awaited?)).should_not be_empty

        # Since we gracefully close the client, we expect each heartbeat
        # to complete.
        started_awaited.length.should == succeeded_awaited.length
        started_regular.length.should == succeeded_regular.length
      end
    end
  end
end
