# frozen_string_literal: true

require 'spec_helper'

describe 'Connections' do
  clean_slate

  let(:client) do
    ClientRegistry.instance.global_client('authorized').tap do |client|
      stop_monitoring(client)
    end
  end

  let(:server) { client.cluster.servers.first }

  describe '#connect!' do
    let(:connection) do
      Mongo::Server::Connection.new(server, server.options)
    end

    describe 'wire protocol version range update' do
      require_no_required_api_version

      let(:client) { ClientRegistry.instance.global_client('authorized').with(app_name: 'wire_protocol_update') }

      context 'non-lb' do
        require_topology :single, :replica_set, :sharded

        it 'updates on handshake response from non-monitoring connections' do
          # connect server
          client['test'].insert_one(test: 1)

          # kill background threads so that they are not interfering with
          # our mocked hello response
          client.cluster.servers.each do |server|
            server.monitor.stop!
          end

          server = client.cluster.servers.first
          expect(server.features.server_wire_versions.max >= 4).to be true
          server.features.server_wire_versions.max

          RSpec::Mocks.with_temporary_scope do
            # now pretend a handshake returned a different range
            features = Mongo::Server::Description::Features.new(0..3)
            # One Features instantiation is for SDAM event publication, this
            # one always happens. The second one happens on servers
            # where we do not negotiate auth mechanism.
            expect(Mongo::Server::Description::Features).to receive(:new).at_least(:once).and_return(features)

            connection = Mongo::Server::Connection.new(server, server.options)
            expect(connection.connect!).to be true

            # hello response should update server description via sdam flow,
            # which includes wire version range
            expect(server.features.server_wire_versions.max).to eq(3)
          end
        end
      end

      context 'lb' do
        require_topology :load_balanced

        it 'does not update on handshake response from non-monitoring connections since there are not any' do
          # connect server
          client['test'].insert_one(test: 1)

          server = client.cluster.servers.first
          server.load_balancer?.should be true
          server.features.server_wire_versions.max.should be 0
        end
      end
    end

    describe 'SDAM flow triggered by hello on non-monitoring thread' do
      # replica sets can transition between having and not having a primary
      require_topology :replica_set

      let(:client) do
        # create a new client because we make manual state changes
        ClientRegistry.instance.global_client('authorized').with(app_name: 'non-monitoring thread sdam')
      end

      it 'performs SDAM flow' do
        client['foo'].insert_one(bar: 1)
        client.cluster.servers_list.each do |server|
          server.monitor.stop!
        end
        expect(client.cluster.topology.class).to eq(Mongo::Cluster::Topology::ReplicaSetWithPrimary)

        # need to connect to the primary for topology to change
        server = client.cluster.servers.detect do |server|
          server.primary?
        end

        # overwrite server description
        server.instance_variable_set(:@description, Mongo::Server::Description.new(
                                                      server.address
                                                    ))

        # overwrite topology
        client.cluster.instance_variable_set(:@topology,
                                             Mongo::Cluster::Topology::ReplicaSetNoPrimary.new(
                                               client.cluster.topology.options, client.cluster.topology.monitoring, client.cluster
                                             ))

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
