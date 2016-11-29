require 'spec_helper'

describe Mongo::Cluster::Topology::ReplicaSet do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
    end
  end

  describe '#servers' do

    let(:mongos) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:standalone) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:replica_set) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:replica_set_two) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:mongos_description) do
      Mongo::Server::Description.new(address, { 'msg' => 'isdbgrid' })
    end

    let(:standalone_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => true })
    end

    let(:replica_set_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => true, 'setName' => 'testing' })
    end

    let(:replica_set_two_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => true, 'setName' => 'test' })
    end

    before do
      mongos.monitor.instance_variable_set(:@description, mongos_description)
      standalone.monitor.instance_variable_set(:@description, standalone_description)
      replica_set.monitor.instance_variable_set(:@description, replica_set_description)
      replica_set_two.monitor.instance_variable_set(:@description, replica_set_two_description)
    end

    context 'when no replica set name is provided' do

      let(:topology) do
        described_class.new({}, monitoring, [])
      end

      let(:servers) do
        topology.servers([ mongos, standalone, replica_set, replica_set_two ])
      end

      it 'returns only replica set members' do
        expect(servers).to eq([ replica_set, replica_set_two ])
      end
    end

    context 'when a replica set name is provided' do

      let(:topology) do
        described_class.new({ :replica_set => 'testing' }, monitoring)
      end

      let(:servers) do
        topology.servers([ mongos, standalone, replica_set, replica_set_two ])
      end

      it 'returns only replica set members is the provided set' do
        expect(servers).to eq([ replica_set ])
      end
    end
  end

  describe '.replica_set?' do

    it 'returns true' do
      expect(described_class.new({}, monitoring)).to be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns false' do
      expect(described_class.new({}, monitoring)).to_not be_sharded
    end
  end

  describe '.single?' do

    it 'returns false' do
      expect(described_class.new({}, monitoring)).to_not be_single
    end
  end

  describe '#has_readable_servers?' do

    let(:topology) do
      described_class.new({}, monitoring, [])
    end

    let(:cluster) do
      double('cluster', servers: servers, single?: false, sharded?: false, unknown?: false)
    end

    context 'when the read preference is primary' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :primary)
      end

      context 'when a primary exists' do

        let(:servers) do
          [ double('server', primary?: true) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a primary does not exist' do

        let(:servers) do
          [ double('server', primary?: false) ]
        end

        it 'returns false' do
          expect(topology).to_not have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is primary preferred' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :primary_preferred)
      end

      context 'when a primary exists' do

        let(:servers) do
          [ double('server', primary?: true, secondary?: false) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a primary does not exist' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is secondary' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :secondary)
      end

      context 'when a secondary exists' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a secondary does not exist' do

        let(:servers) do
          [ double('server', primary?: true, secondary?: false) ]
        end

        it 'returns false' do
          expect(topology).to_not have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is secondary preferred' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :secondary_preferred)
      end

      context 'when a secondary exists' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a secondary does not exist' do

        let(:servers) do
          [ double('server', secondary?: false, primary?: true) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is nearest' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :nearest)
      end

      let(:servers) do
        [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
      end

      it 'returns true' do
        expect(topology).to have_readable_server(cluster, selector)
      end
    end

    context 'when the read preference is not provided' do

      context 'when a primary exists' do

        let(:servers) do
          [ double('server', primary?: true, secondary?: false) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster)
        end
      end

      context 'when a primary does not exist' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns false' do
          expect(topology).to_not have_readable_server(cluster)
        end
      end
    end
  end

  describe '#has_writable_servers?' do

    let(:topology) do
      described_class.new({}, monitoring, [])
    end

    context 'when a primary server exists' do

      let(:primary) do
        double('server', :primary? => true)
      end

      let(:secondary) do
        double('server', :primary? => false)
      end

      let(:cluster) do
        double('cluster', servers: [ primary, secondary ])
      end

      it 'returns true' do
        expect(topology).to have_writable_server(cluster)
      end
    end

    context 'when no primary server exists' do

      let(:server) do
        double('server', :primary? => false)
      end

      let(:cluster) do
        double('cluster', servers: [ server ])
      end

      it 'returns false' do
        expect(topology).to_not have_writable_server(cluster)
      end
    end
  end

  describe '#add_hosts?' do

    let(:primary) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:secondary) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:primary_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => true, 'setName' => 'testing' })
    end

    let(:secondary_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => false, 'secondary' => true,
                                                'setName' => 'testing' })
    end

    let(:topology) do
      described_class.new({ :replica_set => 'testing' }, monitoring)
    end

    before do
      primary.monitor.instance_variable_set(:@description, primary_description)
      secondary.monitor.instance_variable_set(:@description, secondary_description)
    end

    context 'when the list of servers does not include a primary' do

      let(:servers) do
        [ secondary ]
      end

      context 'when the description is a member of the replica set' do

        let(:description) do
          double('description').tap do |d|
            allow(d).to receive(:replica_set_member?).and_return(true)
            allow(d).to receive(:replica_set_name).and_return('testing')
          end
        end

        it 'returns true' do
          expect(topology.add_hosts?(description, servers)).to eq(true)
        end
      end

      context 'when the description is not a member of the replica set' do

        let(:description) do
          double('description').tap do |d|
            allow(d).to receive(:replica_set_member?).and_return(false)
            allow(d).to receive(:primary?).and_return(false)
          end
        end

        it 'returns false' do
          expect(topology.add_hosts?(description, servers)).to eq(false)
        end
      end
    end

    context 'when the list of servers has a primary' do

      let(:servers) do
        [ primary, secondary ]
      end

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:replica_set_member?).and_return(true)
          allow(d).to receive(:replica_set_name).and_return('testing')
          allow(d).to receive(:primary?).and_return(false)
        end
      end

      it 'returns false' do
        expect(topology.add_hosts?(description, servers)).to eq(false)
      end
    end
  end

  describe '#remove_hosts?' do

    let(:primary) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:primary_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => true, 'setName' => 'testing' })
    end

    let(:topology) do
      described_class.new({ :replica_set => 'testing' }, monitoring)
    end

    before do
      primary.monitor.instance_variable_set(:@description, primary_description)
    end

    context 'when the description has an empty config' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:config).and_return({})
        end
      end

      it 'returns false' do
        expect(topology.remove_hosts?(description)).to eq(false)
      end
    end

    context 'when the description is from a primary' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:config).and_return({ 'ismaster' => true })
          allow(d).to receive(:primary?).and_return(true)
        end
      end

      it 'returns true' do
        expect(topology.remove_hosts?(description)).to eq(true)
      end
    end

    context 'when the description has an empty hosts list' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:config).and_return({ 'ismaster' => true })
          allow(d).to receive(:primary?).and_return(false)
          allow(d).to receive(:me_mismatch?).and_return(false)
          allow(d).to receive(:hosts).and_return([])
        end
      end

      it 'returns true' do
        expect(topology.remove_hosts?(description)).to eq(true)
      end
    end

    context 'when the description is not from the replica set' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:config).and_return({ 'ismaster' => true })
          allow(d).to receive(:primary?).and_return(false)
          allow(d).to receive(:hosts).and_return([ primary ])
          allow(d).to receive(:replica_set_name).and_return('test')
          allow(d).to receive(:replica_set_member?).and_return(true)
          allow(d).to receive(:me_mismatch?).and_return(false)
        end
      end

      it 'returns true' do
        expect(topology.remove_hosts?(description)).to eq(true)
      end
    end

  end

  describe '#remove_server?' do

    let(:secondary) do
      Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:secondary_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => false, 'secondary' => true,
                                                'setName' => 'test' })
    end

    let(:topology) do
      described_class.new({ :replica_set => 'testing' }, monitoring)
    end

    before do
      secondary.monitor.instance_variable_set(:@description, secondary_description)
    end

    context 'when the description is from a server that should itself be removed' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:config).and_return({ 'setName' => 'test' })
          allow(d).to receive(:replica_set_member?).and_return(true)
          allow(d).to receive(:replica_set_name).and_return('test')
          allow(d).to receive(:is_server?).and_return(true)
          allow(d).to receive(:ghost?).and_return(false)
        end
      end

      it 'returns true' do
        expect(topology.remove_server?(description, secondary)).to eq(true)
      end
    end

    context 'when the description is a member of the replica set' do

      context 'when the description includes the server in question' do

        let(:description) do
          double('description').tap do |d|
            allow(d).to receive(:config).and_return({ 'setName' => 'testing' })
            allow(d).to receive(:replica_set_member?).and_return(true)
            allow(d).to receive(:replica_set_name).and_return('testing')
            allow(d).to receive(:lists_server?).and_return(true)
          end
        end

        it 'returns false' do
          expect(topology.remove_server?(description, secondary)).to eq(false)
        end
      end

      context 'when the description does not include the server in question' do

        let(:description) do
          double('description').tap do |d|
            allow(d).to receive(:config).and_return({ 'setName' => 'testing' })
            allow(d).to receive(:replica_set_member?).and_return(true)
            allow(d).to receive(:replica_set_name).and_return('testing')
            allow(d).to receive(:is_server?).and_return(false)
            allow(d).to receive(:lists_server?).and_return(false)
          end
        end

        it 'returns true' do
          expect(topology.remove_server?(description, secondary)).to eq(true)
        end
      end
    end

    context 'when the description is not a member of the replica set' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:config).and_return({ 'setName' => 'test' })
          allow(d).to receive(:replica_set_member?).and_return(true)
          allow(d).to receive(:replica_set_name).and_return('test')
          allow(d).to receive(:is_server?).and_return(false)
        end
      end

      it 'returns false' do
        expect(topology.remove_server?(description, secondary)).to eq(false)
      end
    end
  end
end
