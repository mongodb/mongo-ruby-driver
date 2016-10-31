require 'spec_helper'

describe Mongo::Cluster::Topology::Sharded do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:topology) do
    described_class.new({}, monitoring)
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
    end
  end

  let(:mongos) do
    Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
  end

  let(:standalone) do
    Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
  end

  let(:replica_set) do
    Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
  end

  let(:mongos_description) do
    Mongo::Server::Description.new(address, { 'msg' => 'isdbgrid' })
  end

  let(:standalone_description) do
    Mongo::Server::Description.new(address, { 'ismaster' => true })
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

  describe '#add_hosts?' do

    it 'returns false' do
      expect(topology.add_hosts?(double('description'), [])).to eq(false)
    end
  end

  describe '#remove_hosts?' do

    it 'returns true' do
      expect(topology.remove_hosts?(double('description'))).to eq(true)
    end
  end

  describe '#remove_server?' do

    before do
      mongos.monitor.instance_variable_set(:@description, mongos_description)
      replica_set.monitor.instance_variable_set(:@description, replica_set_description)
    end

    context 'when the server itself should be removed' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:mongos?).and_return(false)
          allow(d).to receive(:unknown?).and_return(false)
          allow(d).to receive(:is_server?).and_return(true)
        end
      end

      it 'returns true' do
        expect(topology.remove_server?(description, mongos)).to eq(true)
      end
    end

    context 'when the server is neither a mongos nor an unknown' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:mongos?).and_return(true)
          allow(d).to receive(:is_server?).and_return(false)
        end
      end

      it 'returns true' do
        expect(topology.remove_server?(description, replica_set)).to eq(true)
      end
    end
  end
end
