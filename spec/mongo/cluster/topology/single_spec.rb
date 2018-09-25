require 'spec_helper'

describe Mongo::Cluster::Topology::Single do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:topology) do
    described_class.new({}, monitoring)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:options).and_return({})
    end
  end

  describe '.servers' do

    let(:mongos) do
      Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
    end

    let(:standalone) do
      Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
    end

    let(:standalone_two) do
      Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
    end

    let(:replica_set) do
      Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
    end

    let(:mongos_description) do
      Mongo::Server::Description.new(address, { 'msg' => 'isdbgrid' })
    end

    let(:standalone_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => true, 'ok' => 1 })
    end

    let(:replica_set_description) do
      Mongo::Server::Description.new(address, { 'ismaster' => true, 'setName' => 'testing' })
    end

    before do
      mongos.monitor.instance_variable_set(:@description, mongos_description)
      standalone.monitor.instance_variable_set(:@description, standalone_description)
      standalone_two.monitor.instance_variable_set(:@description, standalone_description)
      replica_set.monitor.instance_variable_set(:@description, replica_set_description)
    end

    let(:servers) do
      topology.servers([ mongos, standalone, standalone_two, replica_set ])
    end

    it 'returns only the first standalone server' do
      expect(servers).to eq([ standalone ])
    end
  end

  describe '.replica_set?' do

    it 'returns false' do
      expect(topology).to_not be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns false' do
      expect(topology).to_not be_sharded
    end
  end

  describe '.single?' do

    it 'returns true' do
      expect(topology).to be_single
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

    it 'returns false' do
      expect(topology.remove_hosts?(double('description'))).to eq(false)
    end
  end

  describe '#remove_server?' do

    it 'returns false' do
      expect(topology.remove_server?(double('description'), double('server'))).to eq(false)
    end
  end
end
