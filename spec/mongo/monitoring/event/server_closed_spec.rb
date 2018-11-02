require 'lite_spec_helper'

describe Mongo::Monitoring::Event::ServerClosed do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:monitoring) { double('monitoring') }

  let(:cluster) do
    double('cluster').tap do |cluster|
      allow(cluster).to receive(:addresses).and_return([address])
    end
  end

  let(:topology) do
    Mongo::Cluster::Topology::Unknown.new({}, monitoring, cluster)
  end

  let(:event) do
    described_class.new(address, topology)
  end

  describe '#summary' do
    it 'renders correctly' do
      expect(event.summary).to eq('#<ServerClosed address=127.0.0.1:27017 topology=Unknown[127.0.0.1:27017]>')
    end
  end
end
