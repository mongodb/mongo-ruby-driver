# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::ServerDescriptionChanged do

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

  let(:previous_desc) { Mongo::Server::Description.new(address) }
  let(:updated_desc) { Mongo::Server::Description.new(address) }

  let(:event) do
    described_class.new(address, topology, previous_desc, updated_desc)
  end

  describe '#summary' do

    it 'renders correctly' do
      expect(event.summary).to eq("#<ServerDescriptionChanged address=127.0.0.1:27017 prev=UNKNOWN new=UNKNOWN>")
    end
  end
end
