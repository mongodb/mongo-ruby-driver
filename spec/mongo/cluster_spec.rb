require 'spec_helper'

describe Mongo::Cluster do

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:cluster_with_semaphore) do
    described_class.new(SpecConfig.instance.addresses, monitoring,
      SpecConfig.instance.test_options.merge(
        server_selection_semaphore: Mongo::Semaphore.new))
  end

  let(:cluster_without_io) do
    described_class.new(SpecConfig.instance.addresses, monitoring,
      SpecConfig.instance.test_options.merge(monitoring_io: false))
  end

  let(:cluster) { cluster_without_io }

  describe '#==' do

    context 'when the other is a cluster' do

      context 'when the addresses are the same' do

        context 'when the options are the same' do

          let(:other) do
            described_class.new(SpecConfig.instance.addresses, monitoring,
              SpecConfig.instance.test_options.merge(monitoring_io: false))
          end

          it 'returns true' do
            expect(cluster_without_io).to eq(other)
          end
        end

        context 'when the options are not the same' do

          let(:other) do
            described_class.new([ '127.0.0.1:27017' ], monitoring,
              SpecConfig.instance.test_options.merge(replica_set: 'test', monitoring_io: false))
          end

          it 'returns false' do
            expect(cluster_without_io).to_not eq(other)
          end
        end
      end

      context 'when the addresses are not the same' do

        let(:other) do
          described_class.new([ '127.0.0.1:27999' ], monitoring,
            SpecConfig.instance.test_options.merge(monitoring_io: false))
        end

        it 'returns false' do
          expect(cluster_without_io).to_not eq(other)
        end
      end
    end

    context 'when the other is not a cluster' do

      it 'returns false' do
        expect(cluster_without_io).to_not eq('test')
      end
    end
  end

  describe '#has_readable_server?' do

    let(:selector) do
      Mongo::ServerSelector.get(mode: :primary)
    end

    it 'delegates to the topology' do
      expect(cluster_without_io.has_readable_server?).to eq(cluster_without_io.topology.has_readable_server?(cluster_without_io))
    end
  end

  describe '#has_writable_server?' do

    it 'delegates to the topology' do
      expect(cluster_without_io.has_writable_server?).to eq(cluster_without_io.topology.has_writable_server?(cluster_without_io))
    end
  end

  describe '#inspect' do

    let(:preference) do
      Mongo::ServerSelector.get(ServerSelector::PRIMARY)
    end

    it 'displays the cluster seeds and topology' do
      expect(cluster_without_io.inspect).to include('topology')
      expect(cluster_without_io.inspect).to include('servers')
    end
  end

  describe '#replica_set_name' do

    let(:preference) do
      Mongo::ServerSelector.get(ServerSelector::PRIMARY)
    end

    context 'when the option is provided' do

      let(:cluster) do
        described_class.new(
          [ '127.0.0.1:27017' ],
          monitoring,
          {monitoring_io: false, connect: :replica_set, replica_set: 'testing'},
        )
      end

      it 'returns the name' do
        expect(cluster.replica_set_name).to eq('testing')
      end
    end

    context 'when the option is not provided' do

      let(:cluster) do
        described_class.new(
          [ '127.0.0.1:27017' ],
          monitoring,
          {monitoring_io: false, connect: :direct},
        )
      end

      it 'returns nil' do
        expect(cluster.replica_set_name).to be_nil
      end
    end
  end

  describe '#scan!' do

    let(:preference) do
      Mongo::ServerSelector.get(ServerSelector::PRIMARY)
    end

    let(:known_servers) do
      cluster.instance_variable_get(:@servers)
    end

    before do
      expect(known_servers.first).to receive(:scan!).and_call_original
    end

    it 'returns true' do
      expect(cluster.scan!).to be true
    end
  end

  describe '#servers' do
    let(:cluster) { cluster_with_semaphore }

    context 'when topology is single' do
      before do
        unless ClusterConfig.instance.single_server?
          skip 'Topology is not a single server'
        end
      end

      context 'when the server is a mongos' do
        require_topology :sharded

        it 'returns the mongos' do
          expect(cluster.servers.size).to eq(1)
        end
      end

      context 'when the server is a replica set member' do
        require_topology :replica_set

        it 'returns the replica set member' do
          expect(cluster.servers.size).to eq(1)
        end
      end
    end

    context 'when the cluster has no servers' do

      let(:servers) do
        []
      end

      before do
        cluster_without_io.instance_variable_set(:@servers, servers)
        cluster_without_io.instance_variable_set(:@topology, topology)
      end

      context 'when topology is Single' do

        let(:topology) do
          Mongo::Cluster::Topology::Single.new({}, monitoring, cluster_without_io)
        end

        it 'returns an empty array' do
          expect(cluster_without_io.servers).to eq([])
        end
      end

      context 'when topology is ReplicaSetNoPrimary' do

        let(:topology) do
          Mongo::Cluster::Topology::ReplicaSetNoPrimary.new({replica_set_name: 'foo'}, monitoring, cluster_without_io)
        end

        it 'returns an empty array' do
          expect(cluster_without_io.servers).to eq([])
        end
      end

      context 'when topology is Sharded' do

        let(:topology) do
          Mongo::Cluster::Topology::Sharded.new({}, monitoring, cluster_without_io)
        end

        it 'returns an empty array' do
          expect(cluster_without_io.servers).to eq([])
        end
      end

      context 'when topology is Unknown' do

        let(:topology) do
          Mongo::Cluster::Topology::Unknown.new({}, monitoring, cluster_without_io)
        end

        it 'returns an empty array' do
          expect(cluster_without_io.servers).to eq([])
        end
      end
    end
  end

  describe '#add' do

    context 'when topology is Single' do

      let(:cluster) { cluster_with_semaphore }

      let(:topology) do
        Mongo::Cluster::Topology::Single.new({}, cluster)
      end

      before do
        cluster.add('a')
      end

      it 'does not add discovered servers to the cluster' do
        expect(cluster.servers[0].address.seed).to_not eq('a')
      end
    end
  end

  describe '#disconnect!' do
    let(:cluster) { cluster_with_semaphore }

    let(:known_servers) do
      cluster.instance_variable_get(:@servers)
    end

    let(:periodic_executor) do
      cluster.instance_variable_get(:@periodic_executor)
    end

    describe 'disconnection' do
      before do
        known_servers.each do |server|
          expect(server).to receive(:disconnect!).and_call_original
        end
        expect(periodic_executor).to receive(:stop!).and_call_original
      end

      it 'disconnects each server and the cursor reaper and returns true' do
        expect(cluster.disconnect!).to be(true)
      end
    end

    describe 'repeated disconnection' do
      before do
        known_servers.each do |server|
          expect(server).to receive(:disconnect!).and_call_original
        end
        expect(periodic_executor).to receive(:stop!).and_call_original
      end

      let(:monitoring) { Mongo::Monitoring.new }
      let(:subscriber) { Mongo::SDAMMonitoring::TestSubscriber.new }

      it 'publishes server closed event once' do
        monitoring.subscribe(Mongo::Monitoring::SERVER_CLOSED, subscriber)
        expect(cluster.disconnect!).to be(true)
        expect(subscriber.first_event('server_closed_event')).not_to be nil
        subscriber.events.clear
        expect(cluster.disconnect!).to be(true)
        expect(subscriber.first_event('server_closed_event')).to be nil
      end
    end
  end

  describe '#reconnect!' do

    let(:cluster) { cluster_with_semaphore }

    let(:periodic_executor) do
      cluster.instance_variable_get(:@periodic_executor)
    end

    before do
      cluster.servers.each do |server|
        expect(server).to receive(:reconnect!).and_call_original
      end
      expect(periodic_executor).to receive(:restart!).and_call_original
    end

    it 'reconnects each server and the cursor reaper and returns true' do
      expect(cluster.reconnect!).to be(true)
    end
  end

  describe '#remove' do

    let(:address_a) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:address_b) do
      Mongo::Address.new('127.0.0.1:27018')
    end

    let(:monitoring) do
      Mongo::Monitoring.new(monitoring: false)
    end

    let(:server_a) do
      Mongo::Server.new(address_a, cluster, monitoring, Mongo::Event::Listeners.new)
    end

    let(:server_b) do
      Mongo::Server.new(address_b, cluster, monitoring, Mongo::Event::Listeners.new)
    end

    let(:servers) do
      [ server_a, server_b ]
    end

    let(:addresses) do
      [ address_a, address_b ]
    end

    before do
      cluster.instance_variable_set(:@servers, servers)
      cluster.remove('127.0.0.1:27017')
    end

    it 'removes the host from the list of servers' do
      expect(cluster.instance_variable_get(:@servers)).to eq([server_b])
    end

    it 'removes the host from the list of addresses' do
      expect(cluster.addresses).to eq([address_b])
    end
  end

  describe '#next_primary' do

    let(:cluster) do
      authorized_client.cluster
    end

    let(:primary_candidates) do
      if cluster.single?
        cluster.servers
      elsif cluster.sharded?
        cluster.servers
      else
        cluster.servers.select { |s| s.primary? }
      end
    end

    it 'always returns the primary, mongos, or standalone' do
      expect(primary_candidates).to include(cluster.next_primary)
    end
  end

  describe '#app_metadata' do

    it 'returns an AppMetadata object' do
      expect(cluster_without_io.app_metadata).to be_a(Mongo::Server::AppMetadata)
    end

    context 'when the client has an app_name set' do

      let(:cluster) do
        authorized_client.with(app_name: 'cluster_test', monitoring_io: false).cluster
      end

      it 'constructs an AppMetadata object with the app_name' do
        expect(cluster.app_metadata.send(:full_client_document)[:application]).to eq('name' => 'cluster_test')
      end
    end

    context 'when the client does not have an app_name set' do

      let(:cluster) do
        authorized_client.cluster
      end

      it 'constructs an AppMetadata object with no app_name' do
        expect(cluster.app_metadata.send(:full_client_document)[:application]).to be_nil
      end
    end
  end

  describe '#cluster_time' do

    let(:operation) do
      client.command(ping: 1)
    end

    let(:operation_with_session) do
      client.command({ ping: 1 }, session: session)
    end

    let(:second_operation) do
      client.command({ ping: 1 }, session: session)
    end

    it_behaves_like 'an operation updating cluster time'
  end

  describe '#update_cluster_time' do

    let(:cluster) do
      described_class.new(SpecConfig.instance.addresses, monitoring,
        SpecConfig.instance.test_options.merge(heartbeat_frequency: 1000, monitoring_io: false))
    end

    let(:result) do
      double('result', cluster_time: cluster_time_doc)
    end

    context 'when the cluster_time variable is nil' do

      before do
        cluster.instance_variable_set(:@cluster_time, nil)
        cluster.update_cluster_time(result)
      end

      context 'when the cluster time received is nil' do

        let(:cluster_time_doc) do
          nil
        end

        it 'does not set the cluster_time variable' do
          expect(cluster.cluster_time).to be_nil
        end
      end

      context 'when the cluster time received is not nil' do

        let(:cluster_time_doc) do
          BSON::Document.new(Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(1, 1))
        end

        it 'sets the cluster_time variable to the cluster time doc' do
          expect(cluster.cluster_time).to eq(cluster_time_doc)
        end
      end
    end

    context 'when the cluster_time variable has a value' do

      before do
        cluster.instance_variable_set(:@cluster_time, BSON::Document.new(
            Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(1, 1)))
        cluster.update_cluster_time(result)
      end

      context 'when the cluster time received is nil' do

        let(:cluster_time_doc) do
          nil
        end

        it 'does not update the cluster_time variable' do
          expect(cluster.cluster_time).to eq(BSON::Document.new(
              Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(1, 1)))
        end
      end

      context 'when the cluster time received is not nil' do

        context 'when the cluster time received is greater than the cluster_time variable' do

          let(:cluster_time_doc) do
            BSON::Document.new(Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(1, 2))
          end

          it 'sets the cluster_time variable to the cluster time' do
            expect(cluster.cluster_time).to eq(cluster_time_doc)
          end
        end

        context 'when the cluster time received is less than the cluster_time variable' do

          let(:cluster_time_doc) do
            BSON::Document.new(Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(0, 1))
          end

          it 'does not set the cluster_time variable to the cluster time' do
            expect(cluster.cluster_time).to eq(BSON::Document.new(
                Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(1, 1)))
          end
        end

        context 'when the cluster time received is equal to the cluster_time variable' do

          let(:cluster_time_doc) do
            BSON::Document.new(Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(1, 1))
          end

          it 'does not change the cluster_time variable' do
            expect(cluster.cluster_time).to eq(BSON::Document.new(
                Mongo::Cluster::CLUSTER_TIME => BSON::Timestamp.new(1, 1)))
          end
        end
      end
    end
  end

  describe '#sessions_supported?' do
    context 'in server < 3.6' do
      max_server_version '3.4'

      context 'in single topology' do
        require_topology :single

        let(:client) { ClientRegistry.instance.global_client('authorized') }

        it 'is false' do
          expect(client.cluster.send(:sessions_supported?)).to be false
        end
      end

      context 'in single topology with replica set name set' do
        require_topology :replica_set

        let(:client) do
          new_local_client([SpecConfig.instance.addresses.first],
            SpecConfig.instance.test_options.merge(
              connect: :direct, replica_set: ClusterConfig.instance.replica_set_name))
        end

        it 'is false' do
          expect(client.cluster.send(:sessions_supported?)).to be false
        end
      end

      context 'in replica set topology' do
        require_topology :replica_set

        let(:client) { ClientRegistry.instance.global_client('authorized') }

        it 'is false' do
          expect(client.cluster.send(:sessions_supported?)).to be false
        end
      end

      context 'in sharded topology' do
        require_topology :sharded

        let(:client) { ClientRegistry.instance.global_client('authorized') }

        it 'is false' do
          expect(client.cluster.send(:sessions_supported?)).to be false
        end
      end
    end

    context 'in server 3.6+' do
      min_server_fcv '3.6'

      context 'in single topology' do
        require_topology :single

        let(:client) { ClientRegistry.instance.global_client('authorized') }

        # Contrary to the session spec, 3.6 and 4.0 standalone servers
        # report a logical session timeout and thus are considered to
        # support sessions
        it 'is true' do
          expect(client.cluster.send(:sessions_supported?)).to be true
        end
      end

      context 'in single topology with replica set name set' do
        require_topology :replica_set

        let(:client) do
          new_local_client([SpecConfig.instance.addresses.first],
            SpecConfig.instance.test_options.merge(
              connect: :direct, replica_set: ClusterConfig.instance.replica_set_name))
        end

        it 'is true' do
          expect(client.cluster.send(:sessions_supported?)).to be true
        end
      end

      context 'in replica set topology' do
        require_topology :replica_set

        let(:client) { ClientRegistry.instance.global_client('authorized') }

        it 'is true' do
          expect(client.cluster.send(:sessions_supported?)).to be true
        end
      end

      context 'in sharded topology' do
        require_topology :sharded

        let(:client) { ClientRegistry.instance.global_client('authorized') }

        it 'is true' do
          expect(client.cluster.send(:sessions_supported?)).to be true
        end
      end
    end
  end

  describe '#summary' do
    context 'cluster has unknown servers' do
      it 'includes unknown servers' do
        cluster.servers_list.each do |server|
          expect(server).to be_unknown
        end

        expect(cluster.summary).to match(/Server address=localhost/)
      end
    end

    context 'cluster has known servers' do
      let(:client) { ClientRegistry.instance.global_client('authorized') }
      let(:cluster) { client.cluster }

      before do
        wait_for_all_servers(cluster)
      end

      it 'includes known servers' do
        cluster.servers_list.each do |server|
          expect(server).not_to be_unknown
        end

        expect(cluster.summary).to match(/Server address=localhost/)
      end
    end
  end
end
