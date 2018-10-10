require 'spec_helper'

describe 'Connections' do
  let(:client) { ClientRegistry.instance.global_client('authorized') }
  let(:server) { client.cluster.servers.first }

  describe '#connect!' do
    context 'network error during handshake' do
      let(:connection) do
        Mongo::Server::Connection.new(server, server.options)
      end

      let(:exception) { Mongo::Error::SocketError }

      let(:error) do
        connection
        expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(exception)
        expect do
          connection.connect!
        end.to raise_error(exception)
      end

      it 'sets server type to unknown' do
        expect(server).not_to be_unknown
        error

        expect(server).to be_unknown
      end

      context 'with sdam event subscription' do
        let(:subscriber) { Mongo::SDAMMonitoring::TestSubscriber.new }
        let(:client) do
          ClientRegistry.instance.global_client('authorized').with(app_name: 'connection_integration').tap do |client|
            client.subscribe(Mongo::Monitoring::SERVER_OPENING, subscriber)
            client.subscribe(Mongo::Monitoring::SERVER_CLOSED, subscriber)
            client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, subscriber)
            client.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING, subscriber)
            client.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED, subscriber)
          end
        end

        it 'publishes server description changed event' do
          expect(subscriber.events).to be_empty

          connection
          subscriber.events.clear
          error

          event = subscriber.first_event('server_description_changed_event')
          expect(event).not_to be_nil
          expect(event.address).to eq(server.address)
          expect(event.new_description).to be_unknown
        end
      end

      context 'error during handshake to primary in a replica set' do
        require_topology 'replica_set'

        let(:server) { client.cluster.servers.detect { |server| server.primary? } }

        before do
          # insert to perform server selection and get topology to primary
          client[:test].insert_one(foo: 'bar')
        end

        it 'sets cluster type to replica set without primary' do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetWithPrimary)
          error

          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
        end
      end
    end

    describe 'wire protocol version range update' do
      # 3.2 wire protocol is 4.
      # Wire protocol < 2 means only scram auth is available,
      # which is not supported by modern mongos.
      # Instead of mucking with this we just limit this test to 3.2+
      # so that we can downgrade protocol range to 0..3 instead of 0..1.
      min_server_version '3.2'

      let(:client) { ClientRegistry.instance.global_client('authorized').with(app_name: 'wire_protocol_update') }

      it 'does not update on ismaster response from non-monitoring connections' do
        # connect server
        client['test'].insert_one(test: 1)

        # kill background threads so that they are not interfering with
        # our mocked ismaster response
        client.cluster.servers.each do |server|
          server.monitor.stop!
        end

        server = client.cluster.servers.first
        expect(server.features.server_wire_versions.max >= 4).to be true
        max_version = server.features.server_wire_versions.max

        # now pretend an ismaster returned a different range
        features = Mongo::Server::Description::Features.new(0..3)
        expect(Mongo::Server::Description::Features).to receive(:new).and_return(features)

        connection = Mongo::Server::Connection.new(server, server.options)
        expect(connection.connect!).to be true

        # ismaster response should not update wire version range stored
        # in description
        expect(server.features.server_wire_versions.max).to eq(max_version)
      end
    end
  end
end
