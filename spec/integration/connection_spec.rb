require 'spec_helper'

describe 'Connections' do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  let(:client) { ClientRegistry.instance.global_client('authorized') }
  let(:server) { client.cluster.servers.first }

  describe '#connect!' do
    fails_on_jruby

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

          wait_for_all_servers(client.cluster)

          connection
          subscriber.events.clear
          error

          event = subscriber.first_event('server_description_changed_event')
          expect(event).not_to be_nil
          expect(event.address).to eq(server.address)
          expect(event.new_description).to be_unknown
        end

        it 'marks server unknown' do
          expect(server).not_to be_unknown

          connection
          error

          expect(server).to be_unknown
        end

        context 'in replica set topology' do
          require_topology :replica_set

          # need to use the primary here, otherwise a secondary will be
          # changed to unknown which wouldn't alter topology
          let(:server) { client.cluster.servers.detect { |s| s.primary? } }

          it 'changes topology type' do
            # wait for topology to get discovered
            client.database.command(ismaster: 1)

            expect(client.cluster.topology.class).to eql(Mongo::Cluster::Topology::ReplicaSetWithPrimary)

            # stop background monitoring to prevent it from racing with the test
            client.cluster.servers.each do |server|
              server.monitor.stop!(true)
            end

            connection
            error

            expect(client.cluster.topology.class).to eql(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
          end
        end
      end

      context 'error during handshake to primary in a replica set' do
        require_topology :replica_set

        let(:server) { client.cluster.servers.detect { |server| server.primary? } }

        before do
          ClientRegistry.instance.close_all_clients
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
      min_server_fcv '3.2'

      let(:client) { ClientRegistry.instance.global_client('authorized').with(app_name: 'wire_protocol_update') }

      it 'updates on ismaster response from non-monitoring connections' do
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

        # Depending on server version, ismaster here may return a
        # description that compares equal to the one we got from a
        # monitoring connection (pre-4.2) or not (4.2+).
        # Since we do run SDAM flow on ismaster responses on
        # non-monitoring connections, force descriptions to be different
        # by setting the existing description here to unknown.
        server.monitor.instance_variable_set('@description',
          Mongo::Server::Description.new(server.address))

        # now pretend an ismaster returned a different range
        features = Mongo::Server::Description::Features.new(0..3)
        # the second Features instantiation is for SDAM event publication
        expect(Mongo::Server::Description::Features).to receive(:new).twice.and_return(features)

        connection = Mongo::Server::Connection.new(server, server.options)
        expect(connection.connect!).to be true

        # ismaster response should update server description via sdam flow,
        # which includes wire version range
        expect(server.features.server_wire_versions.max).to eq(3)
      end
    end

    describe 'SDAM flow triggered by ismaster on non-monitoring thread' do
      # replica sets can transition between having and not having a primary
      require_topology :replica_set

      let(:client) do
        # create a new client because we make manual state changes
        ClientRegistry.instance.global_client('authorized').with(app_name: 'non-monitoring thread sdam')
      end

      it 'performs SDAM flow' do
        client['foo'].insert_one(bar: 1)
        client.cluster.servers.each do |server|
          server.monitor.stop!(true)
        end
        expect(client.cluster.topology.class).to eq(Mongo::Cluster::Topology::ReplicaSetWithPrimary)

        # need to connect to the primary for topology to change
        server = client.cluster.servers.detect do |server|
          server.primary?
        end

        # overwrite server description
        server.monitor.instance_variable_set('@description', Mongo::Server::Description.new(
          server.address))

        # overwrite topology
        client.cluster.instance_variable_set('@topology',
          Mongo::Cluster::Topology::ReplicaSetNoPrimary.new(
            client.cluster.topology.options, client.cluster.topology.monitoring, client.cluster))

        # now create a connection.
        connection = Mongo::Server::Connection.new(server, server.options)

        # verify everything once again
        expect(server).to be_unknown
        expect(client.cluster.topology.class).to eq(Mongo::Cluster::Topology::ReplicaSetNoPrimary)

        # this should dispatch the sdam event
        expect(connection.connect!).to be true

        # back to primary
        expect(server).to be_primary
        expect(client.cluster.topology.class).to eq(Mongo::Cluster::Topology::ReplicaSetWithPrimary)
      end
    end
  end
end
