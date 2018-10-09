require 'spec_helper'

describe 'SDAM events' do
  let(:subscriber) { Mongo::SDAMMonitoring::TestSubscriber.new }

  before do
    ClientRegistry.instance.close_all_clients
  end

  describe 'server closed event' do
    it 'is published when client is closed' do
      client = ClientRegistry.instance.new_local_client(
        SpecConfig.instance.addresses, SpecConfig.instance.test_options)
      client.subscribe(Mongo::Monitoring::SERVER_CLOSED, subscriber)

      # get the client connected
      client.database.command(ismaster: 1)
      expect(subscriber.events).to be_empty

      client.close

      expect(subscriber.events).not_to be_empty
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
      expect(subscriber.events).to be_empty

      client.close

      expect(subscriber.events).not_to be_empty
      event = subscriber.first_event('topology_closed_event')
      expect(event).not_to be_nil

      expect(event.topology).to eql(client.cluster.topology)
    end
  end
end
