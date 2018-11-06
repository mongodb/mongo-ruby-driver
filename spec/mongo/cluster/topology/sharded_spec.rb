require 'spec_helper'

describe Mongo::Cluster::Topology::Sharded do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  # Cluster needs a topology and topology needs a cluster...
  # This temporary cluster is used for topology construction.
  let(:temp_cluster) do
    double('temp cluster').tap do |cluster|
      allow(cluster).to receive(:servers_list).and_return([])
    end
  end

  let(:topology) do
    described_class.new({}, monitoring, temp_cluster)
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
    end
  end

  let(:mongos) do
    Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
  end

  let(:standalone) do
    Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
  end

  let(:replica_set) do
    Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
  end

  let(:mongos_description) do
    Mongo::Server::Description.new(address, { 'msg' => 'isdbgrid', 'ok' => 1 })
  end

  let(:standalone_description) do
    Mongo::Server::Description.new(address, { 'ismaster' => true, 'ok' => 1 })
  end

  let(:replica_set_description) do
    Mongo::Server::Description.new(address, { 'ismaster' => true, 'setName' => 'testing', 'ok' => 1 })
  end

  describe '.servers' do

    before do
      mongos.monitor.instance_variable_set(:@description, mongos_description)
      standalone.monitor.instance_variable_set(:@description, standalone_description)
      replica_set.monitor.instance_variable_set(:@description, replica_set_description)
    end

    let(:servers) do
      topology.servers([ mongos, standalone, replica_set ])
    end

    it 'returns only mongos servers' do
      expect(servers).to eq([ mongos ])
    end
  end

  describe '.replica_set?' do

    it 'returns false' do
      expect(topology).to_not be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns true' do
      expect(topology).to be_sharded
    end
  end

  describe '.single?' do

    it 'returns false' do
      expect(topology).to_not be_single
    end
  end

  describe '#has_readable_servers?' do

    it 'returns true' do
      expect(topology).to have_readable_server(nil, nil)
    end
  end

  describe '#has_writable_servers?' do

    it 'returns true' do
      expect(topology).to have_writable_server(nil)
    end
  end

  describe '#summary' do
    let(:desc1) do
      Mongo::Server::Description.new(Mongo::Address.new('127.0.0.2:27017'))
    end

    let(:desc2) do
      Mongo::Server::Description.new(Mongo::Address.new('127.0.0.2:27027'))
    end

    it 'renders correctly' do
      expect(topology).to receive(:server_descriptions).and_return([desc1, desc2])
      expect(topology.summary).to eq('Sharded[127.0.0.2:27017,127.0.0.2:27027]')
    end
  end
end
