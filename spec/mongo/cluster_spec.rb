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

    it 'sets the initial nodes for the addresses' do
      expect(cluster.nodes).to eq(nodes)
    end
  end
end
