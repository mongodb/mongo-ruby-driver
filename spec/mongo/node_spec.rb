require 'spec_helper'

describe Mongo::Node do

  describe '#initialize' do

    let(:cluster) do
      double('cluster')
    end

    let(:address) do
      '127.0.0.1:27017'
    end

    let(:node) do
      described_class.new(cluster, address, :refresh_interval => 5)
    end

    it 'sets the address' do
      expect(node.address).to eq(address)
    end

    it 'sets the cluster' do
      expect(node.cluster).to eq(cluster)
    end

    it 'sets the options' do
      expect(node.options).to eq(:refresh_interval => 5)
    end
  end

  describe '#refresh' do

    context 'when the server is a single node' do

    end

    context 'when the server is a replica set' do

    end

    context 'when the server is mongos' do

    end
  end

  describe '#refresh_interval' do

    let(:cluster) do
      double('cluster')
    end

    let(:address) do
      '127.0.0.1:27017'
    end

    context 'when an option is provided' do

      let(:node) do
        described_class.new(cluster, address, :refresh_interval => 10)
      end

      it 'returns the option' do
        expect(node.refresh_interval).to eq(10)
      end
    end

    context 'when no option is provided' do

      let(:node) do
        described_class.new(cluster, address)
      end

      it 'defaults to 5' do
        expect(node.refresh_interval).to eq(5)
      end
    end
  end
end
