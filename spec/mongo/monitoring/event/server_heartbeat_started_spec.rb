# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::ServerHeartbeatStarted do

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
    described_class.new(address)
  end

  describe '#summary' do
    it 'renders correctly' do
      expect(event.summary).to eq('#<ServerHeartbeatStarted address=127.0.0.1:27017>')
    end
  end
end
