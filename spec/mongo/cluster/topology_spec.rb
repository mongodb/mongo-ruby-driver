require 'spec_helper'

describe Mongo::Cluster::Topology do

  describe '.initial' do

    context 'when provided a replica set option' do

      let(:topology) do
        described_class.initial([ 'a' ], connect: :replica_set)
      end

      it 'returns a replica set topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSet)
      end
    end

    context 'when provided a single option' do

      let(:topology) do
        described_class.initial([ 'a' ], connect: :direct)
      end

      it 'returns a single topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::Single)
      end

      it 'sets the seed on the topology' do
        expect(topology.seed).to eq('a')
      end
    end

    context 'when provided a sharded option' do

      let(:topology) do
        described_class.initial([ 'a' ], connect: :sharded)
      end

      it 'returns a sharded topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::Sharded)
      end
    end

    context 'when provided no option' do

      context 'when a set name is in the options' do

        let(:topology) do
          described_class.initial([], replica_set: 'testing')
        end

        it 'returns a replica set topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSet)
        end
      end

      context 'when no set name is in the options' do

        let(:topology) do
          described_class.initial([], {})
        end

        it 'returns an unknown topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Unknown)
        end
      end

      context 'when provided a single mongos', if: single_mongos? do

        let(:topology) do
          described_class.initial(ADDRESSES, TEST_OPTIONS)
        end

        it 'returns a sharded topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Sharded)
        end
      end

      context 'when provided a single replica set member', if: single_rs_member? do

        let(:topology) do
          described_class.initial(ADDRESSES, TEST_OPTIONS)
        end

        it 'returns a single topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Single)
        end
      end
    end
  end
end
