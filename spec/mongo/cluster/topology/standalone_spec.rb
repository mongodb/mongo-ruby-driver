require 'spec_helper'

describe Mongo::Cluster::Topology::Standalone do

  describe '.servers' do

    let(:mongos) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:standalone) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:standalone_two) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:replica_set) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:mongos_description) do
      Mongo::Server::Description.new(mongos, { 'msg' => 'isdbgrid' })
    end

    let(:standalone_description) do
      Mongo::Server::Description.new(standalone, { 'ismaster' => true })
    end

    let(:replica_set_description) do
      Mongo::Server::Description.new(replica_set, { 'ismaster' => true, 'setName' => 'testing' })
    end

    before do
      mongos.instance_variable_set(:@description, mongos_description)
      standalone.instance_variable_set(:@description, standalone_description)
      standalone_two.instance_variable_set(:@description, standalone_description)
      replica_set.instance_variable_set(:@description, replica_set_description)
    end

    let(:servers) do
      described_class.servers([ mongos, standalone, standalone_two, replica_set ])
    end

    it 'returns only the first standalone server' do
      expect(servers).to eq([ standalone ])
    end
  end

  describe '.replica_set?' do

    it 'returns false' do
      expect(described_class).to_not be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns false' do
      expect(described_class).to_not be_sharded
    end
  end

  describe '.standalone?' do

    it 'returns true' do
      expect(described_class).to be_standalone
    end
  end
end
