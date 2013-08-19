require 'spec_helper'

describe Mongo::Cluster do

  describe '#==' do

    let(:addresses) do
      ['127.0.0.1:27017', '127.0.0.1:27019']
    end

    let(:cluster) do
      described_class.new(addresses)
    end

    context 'when the other is not a cluster' do

      it 'returns false' do
        expect(cluster).to_not eq('test')
      end
    end

    context 'when the other is a cluster' do

      context 'when the nodes are equal' do

        let(:other) do
          described_class.new(addresses)
        end

        it 'returns true' do
          expect(cluster).to eq(other)
        end
      end

      context 'when the nodes are not equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27021'])
        end

        it 'returns false' do
          expect(cluster).to_not eq(other)
        end
      end
    end
  end

  describe '#add' do

    let(:addresses) do
      ['127.0.0.1:27017', '127.0.0.1:27019']
    end

    let(:cluster) do
      described_class.new(addresses)
    end

    context 'when a node with the address does not exist' do

      let(:address) do
        '127.0.0.1:27020'
      end

      let!(:added) do
        cluster.add(address)
      end

      it 'adds the node to the cluster' do
        expect(cluster.nodes.size).to eq(3)
      end

      it 'returns the newly added node' do
        expect(added.address).to eq(address)
      end
    end

    context 'when a node with the address exists' do

      let!(:added) do
        cluster.add('127.0.0.1:27017')
      end

      it 'does not add the node to the cluster' do
        expect(cluster.nodes.size).to eq(2)
      end

      it 'returns nil' do
        expect(added).to be_nil
      end
    end
  end

  describe '#initialize' do

    let(:addresses) do
      ['127.0.0.1:27017', '127.0.0.1:27019']
    end

    let(:nodes) do
      addresses.map { |address| Mongo::Node.new(address) }
    end

    let(:cluster) do
      described_class.new(addresses)
    end

    it 'sets the configured addresses' do
      expect(cluster.addresses).to eq(addresses)
    end
  end

  describe '#nodes' do

    let(:addresses) do
      ['127.0.0.1:27017', '127.0.0.1:27019']
    end

    let(:cluster) do
      described_class.new(addresses)
    end

    let(:nodes_internal) do
      cluster.instance_variable_get(:@nodes)
    end

    context 'when all nodes are alive' do

      before do
        expect(nodes_internal.first).to receive(:operable?).and_return(true)
        expect(nodes_internal.last).to receive(:operable?).and_return(true)
      end

      it 'returns all nodes' do
        expect(cluster.nodes.size).to eq(2)
      end
    end

    context 'when some nodes are not alive' do

      before do
        expect(nodes_internal.first).to receive(:operable?).and_return(true)
        expect(nodes_internal.last).to receive(:operable?).and_return(false)
      end

      it 'returns all alive nodes' do
        expect(cluster.nodes.size).to eq(1)
      end
    end
  end
end
