require 'lite_spec_helper'

describe Mongo::Monitoring::Event::TopologyOpening do

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

  let(:topology) do
    Mongo::Cluster::Topology::Unknown.new({}, monitoring, cluster)
  end

  let(:event) do
    described_class.new(topology)
  end

  describe '#summary' do
    skip_if_linting

    it 'renders correctly' do
      expect(topology).to receive(:server_descriptions).and_return({
        '127.0.0.1:27017' => Mongo::Server::Description.new(Mongo::Address.new('127.0.0.1:27017'))})
      expect(event.summary).to eq('#<TopologyOpening topology=Unknown[127.0.0.1:27017]>')
    end
  end
end
