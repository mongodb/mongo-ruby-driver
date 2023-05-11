# frozen_string_literal: true
# rubocop:todo all

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

    context 'network error during handshake' do
      # On JRuby 9.2.7.0, this line:
      # expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(exception)
      # ... appears to produce a moment in which Mongo::Socket#write is undefined
      # entirely, resulting in this failure:
      # RSpec::Expectations::ExpectationNotMetError: expected Mongo::Error::SocketError, got #<NameError: undefined method `write' for class `Mongo::Socket'>
      fails_on_jruby

      # 4.4 has two monitors and thus our socket mocks get hit twice
      max_server_version '4.2'

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

        let(:subscriber) { Mrss::EventSubscriber.new }
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
          expect(subscriber.succeeded_events).to be_empty

          wait_for_all_servers(client.cluster)

          connection
          subscriber.succeeded_events.clear
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
          let(:server) { client.cluster.next_primary }

          it 'changes topology type' do
            # wait for topology to get discovered
            client.cluster.next_primary

            expect(client.cluster.topology.class).to eql(Mongo::Cluster::Topology::ReplicaSetWithPrimary)

            # stop background monitoring to prevent it from racing with the test
            client.cluster.servers_list.each do |server|
              server.monitor.stop!
            end

            connection
            error

            expect(client.cluster.topology.class).to eql(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
          end
        end
      end

      context 'error during handshake to primary in a replica set' do
        require_topology :replica_set

        let(:server) { client.cluster.next_primary }

        before do
          # insert to perform server selection and get topology to primary
          client.cluster.next_primary
        end

        it 'sets cluster type to replica set without primary' do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetWithPrimary)
          error

          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
        end
      end

      describe 'number of sockets created' do

        before do
          server
        end

        shared_examples_for 'is 1 per connection' do
          it 'is 1 per connection' do
            # Instantiating a connection object should not create any sockets
            RSpec::Mocks.with_temporary_scope do
              expect(socket_cls).not_to receive(:new)

              connection
            end

            # When the connection connects, exactly one socket should be created
            # (and subsequently connected)
            RSpec::Mocks.with_temporary_scope do
              expect(socket_cls).to receive(:new).and_call_original

              connection.connect!
            end
          end
        end

        let(:socket_cls) { ::Socket }

        it_behaves_like 'is 1 per connection'

        context 'connection to Unix domain socket' do
          # Server does not allow Unix socket connections when TLS is enabled
          require_no_tls

          let(:port) { SpecConfig.instance.any_port }

          let(:client) do
            new_local_client(["/tmp/mongodb-#{port}.sock"], connect: :direct).tap do |client|
              stop_monitoring(client)
            end
          end

          let(:socket_cls) { ::UNIXSocket }

          it_behaves_like 'is 1 per connection'
        end
      end

      context 'when socket connection fails' do

        before do
          server
        end

        let(:socket_cls) { ::Socket }

        let(:socket) do
          double('socket').tap do |socket|
            allow(socket).to receive(:setsockopt)
            allow(socket).to receive(:set_encoding)
            allow(socket).to receive(:getsockopt)
            expect(socket).to receive(:connect).and_raise(IOError, 'test error')

            # This test is testing for the close call:
            expect(socket).to receive(:close)
          end
        end

        it 'closes the socket' do
          RSpec::Mocks.with_temporary_scope do
            expect(::Socket).to receive(:new).with(
              Socket::AF_INET, Socket::SOCK_STREAM, 0).and_return(socket)

            lambda do
              connection.connect!
            end.should raise_error(Mongo::Error::SocketError, /test error/)
          end
        end

        context 'with tls' do
          require_tls

          let(:socket) do
            double('socket').tap do |socket|
              allow(socket).to receive(:hostname=)
              allow(socket).to receive(:sync_close=)
              expect(socket).to receive(:connect).and_raise(IOError, 'test error')

              # This test is testing for the close call:
              expect(socket).to receive(:close)
            end
          end

          it 'closes the SSL socket' do
            RSpec::Mocks.with_temporary_scope do
              expect(OpenSSL::SSL::SSLSocket).to receive(:new).and_return(socket)

              lambda do
                connection.connect!
              end.should raise_error(Mongo::Error::SocketError, /test error/)
            end
          end
        end
      end
    end

    describe 'wire protocol version range update' do
      require_no_required_api_version

      # 3.2 wire protocol is 4.
      # Wire protocol < 2 means only scram auth is available,
      # which is not supported by modern mongos.
      # Instead of mucking with this we just limit this test to 3.2+
      # so that we can downgrade protocol range to 0..3 instead of 0..1.
      min_server_fcv '3.2'

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
          max_version = server.features.server_wire_versions.max

          # Depending on server version, handshake here may return a
          # description that compares equal to the one we got from a
          # monitoring connection (pre-4.2) or not (4.2+).
          # Since we do run SDAM flow on handshake responses on
          # non-monitoring connections, force descriptions to be different
          # by setting the existing description here to unknown.
          server.monitor.instance_variable_set('@description',
            Mongo::Server::Description.new(server.address))

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
        server.instance_variable_set('@description', Mongo::Server::Description.new(
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
