require 'spec_helper'

describe Mongo::Cluster::Topology::Sharded do

  describe '.servers' do

    let(:mongos) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:standalone) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:replica_set) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:mongos_description) do
      Mongo::Server::Description.new({ 'msg' => 'isdbgrid' })
    end

    let(:standalone_description) do
      Mongo::Server::Description.new({ 'ismaster' => true })
    end

    let(:replica_set_description) do
      Mongo::Server::Description.new({ 'ismaster' => true, 'setName' => 'testing' })
    end

    before do
      mongos.instance_variable_set(:@description, mongos_description)
      standalone.instance_variable_set(:@description, standalone_description)
      replica_set.instance_variable_set(:@description, replica_set_description)
    end

    let(:servers) do
      described_class.servers([ mongos, standalone, replica_set ])
    end

    it 'returns only mongos servers' do
      expect(servers).to eq([ mongos ])
    end
  end

  describe '.replica_set?' do

    it 'returns false' do
      expect(described_class).to_not be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns true' do
      expect(described_class).to be_sharded
    end
  end

  describe '.standalone?' do

    it 'returns false' do
      expect(described_class).to_not be_standalone
    end
  end
end
