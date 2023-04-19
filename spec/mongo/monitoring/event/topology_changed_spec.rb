# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::TopologyChanged do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:monitoring) { double('monitoring') }

  let(:cluster) do
    double('cluster').tap do |cluster|
      allow(cluster).to receive(:addresses).and_return([address])
      allow(cluster).to receive(:servers_list).and_return([])
    end
  end

  let(:prev_topology) do
    Mongo::Cluster::Topology::Unknown.new({}, monitoring, cluster)
  end

  let(:new_topology) do
    Mongo::Cluster::Topology::Unknown.new({}, monitoring, cluster)
  end

  let(:event) do
    described_class.new(prev_topology, new_topology)
  end

  describe '#summary' do
    require_no_linting

    it 'renders correctly' do
      expect(prev_topology).to receive(:server_descriptions).and_return({
        '127.0.0.1:27017' => Mongo::Server::Description.new(Mongo::Address.new('127.0.0.1:27017'))})
      expect(new_topology).to receive(:server_descriptions).and_return({
        '127.0.0.1:99999' => Mongo::Server::Description.new(Mongo::Address.new('127.0.0.1:99999'))})
      expect(event.summary).to eq('#<TopologyChanged prev=Unknown[127.0.0.1:27017] new=Unknown[127.0.0.1:99999]>')
    end
  end
end
