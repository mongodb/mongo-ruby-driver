# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Cluster::Topology::Unknown do

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
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

  describe '#initialize' do
    let(:topology) do
      Mongo::Cluster::Topology::Unknown.new(
        {replica_set_name: 'foo'},
        monitoring, temp_cluster)
    end

    it 'does not accept RS name' do
      expect do
        topology
      end.to raise_error(ArgumentError, 'Topology Mongo::Cluster::Topology::Unknown cannot have the :replica_set_name option set')
    end
  end

  describe '.servers' do

    let(:servers) do
      topology.servers([ double('mongos'), double('standalone') ])
    end

    it 'returns an empty array' do
      expect(servers).to eq([ ])
    end
  end

  describe '.replica_set?' do

    it 'returns false' do
      expect(topology).to_not be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns false' do
      expect(topology).not_to be_sharded
    end
  end

  describe '.single?' do

    it 'returns false' do
      expect(topology).not_to be_single
    end
  end

  describe '.unknown?' do

    it 'returns true' do
      expect(topology.unknown?).to be(true)
    end
  end

  describe '#has_readable_servers?' do

    it 'returns false' do
      expect(topology).to_not have_readable_server(nil, nil)
    end
  end

  describe '#has_writable_servers?' do

    it 'returns false' do
      expect(topology).to_not have_writable_server(nil)
    end
  end

  describe '#summary' do
    require_no_linting

    let(:desc) do
      Mongo::Server::Description.new(Mongo::Address.new('127.0.0.2:27017'))
    end

    it 'renders correctly' do
      expect(topology).to receive(:server_descriptions).and_return({desc.address.to_s => desc})
      expect(topology.summary).to eq('Unknown[127.0.0.2:27017]')
    end
  end
end
