require 'spec_helper'

describe Mongo::Node do

  describe '#alive?' do

    let(:cluster) do
      double('cluster')
    end

    let(:address) do
      '127.0.0.1:27017'
    end

    let(:node) do
      described_class.new(cluster, address)
    end

    context 'when the node has been refreshed' do

      context 'when the node is alive' do

        before do
          node.instance_variable_set(:@alive, true)
        end

        it 'returns true' do
          expect(node).to be_alive
        end
      end

      context 'when the node is not alive' do

        before do
          node.instance_variable_set(:@alive, false)
        end

        it 'returns false' do
          expect(node).to_not be_alive
        end
      end
    end

    context 'when the node has not been refreshed' do

      it 'returns false' do
        expect(node).to_not be_alive
      end
    end
  end

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

    let(:cluster) do
      double('cluster')
    end

    let(:address) do
      '127.0.0.1:27017'
    end

    context 'when the server is a single node' do

      let(:node) do
        described_class.new(cluster, address)
      end

      context 'when the node is available' do

        it 'flags the node as master' do

        end

        it 'flags the mode as operable' do

        end

        it 'sets the node latency' do

        end
      end

      context 'when the node is down' do

        it 'flags the node as down' do

        end

        it 'does not flag the node as operable' do

        end

        it 'removes the node latency' do

        end
      end
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
