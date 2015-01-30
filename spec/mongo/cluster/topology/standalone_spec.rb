require 'spec_helper'

describe Mongo::Cluster::Topology::Standalone do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:topology) do
    described_class.new({})
  end

  describe '.servers' do

    let(:mongos) do
      Mongo::Server.new(address, Mongo::Event::Listeners.new)
    end

    let(:standalone) do
      Mongo::Server.new(address, Mongo::Event::Listeners.new)
    end

    let(:standalone_two) do
      Mongo::Server.new(address, Mongo::Event::Listeners.new)
    end

    let(:replica_set) do
      Mongo::Server.new(address, Mongo::Event::Listeners.new)
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

  describe '.standalone?' do

    it 'returns true' do
      expect(topology).to be_standalone
    end
  end
end
