require 'spec_helper'

describe Mongo::Cluster::Topology do

  describe '.initial' do

    context 'when provided a replica set option' do

      let(:topology) do
        described_class.initial(connect: :replica_set)
      end

      it 'returns a replica set topology' do
        expect(topology).to eq(Mongo::Cluster::Topology::ReplicaSet)
      end
    end

    context 'when provided a standalone option' do

      let(:topology) do
        described_class.initial(connect: :direct)
      end

      it 'returns a standalone topology' do
        expect(topology).to eq(Mongo::Cluster::Topology::Standalone)
      end
    end

    context 'when provided a sharded option' do

      let(:topology) do
        described_class.initial(connect: :sharded)
      end

      it 'returns a sharded topology' do
        expect(topology).to eq(Mongo::Cluster::Topology::Sharded)
      end
    end

    context 'when provided no option' do

      context 'when a set name is in the options' do

        let(:topology) do
          described_class.initial(replica_set: 'testing')
        end

        it 'returns a replica set topology' do
          expect(topology).to eq(Mongo::Cluster::Topology::ReplicaSet)
        end
      end

      context 'when no set name is in the options' do

        let(:topology) do
          described_class.initial({})
        end

        it 'returns a standalone topology' do
          expect(topology).to eq(Mongo::Cluster::Topology::Standalone)
        end
      end
    end
  end
end
