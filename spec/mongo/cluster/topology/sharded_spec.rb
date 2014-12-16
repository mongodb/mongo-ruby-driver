require 'spec_helper'

describe Mongo::Cluster::Topology::Sharded do

  describe '.servers' do

    let(:mongos) do
      Mongo::Server.new('127.0.0.1:27017')
    end

    let(:standalone) do
      Mongo::Server.new('127.0.0.1:27017')
    end

    let(:replica_set) do
      Mongo::Server.new('127.0.0.1:27017')
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
      replica_set.instance_variable_set(:@description, replica_set_description)
    end

    let(:servers) do
      described_class.servers([ mongos, standalone, replica_set ])
    end

    it 'returns only mongos servers' do
      expect(servers).to eq([ mongos ])
    end
  end

  describe '.sharded?' do

    it 'returns true' do
      expect(described_class).to be_sharded
    end
  end
end
