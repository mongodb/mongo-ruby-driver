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
  end
end
