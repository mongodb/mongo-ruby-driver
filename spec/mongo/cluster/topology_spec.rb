require 'spec_helper'

describe Mongo::Cluster::Topology do

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  describe '.initial' do

    context 'when provided a replica set option' do

      let(:topology) do
        described_class.initial([ 'a' ], monitoring, connect: :replica_set)
      end

      it 'returns a replica set topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSet)
      end

      context 'when the option is a String (due to YAML parsing)' do

        let(:topology) do
          described_class.initial([ 'a' ], monitoring, connect: 'replica_set')
        end

        it 'returns a replica set topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSet)
        end
      end
    end

    context 'when provided a single option' do

      let(:topology) do
        described_class.initial([ 'a' ], monitoring, connect: :direct)
      end

      it 'returns a single topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::Single)
      end

      it 'sets the seed on the topology' do
        expect(topology.seed).to eq('a')
      end

      context 'when the option is a String (due to YAML parsing)' do

        let(:topology) do
          described_class.initial([ 'a' ], monitoring, connect: 'direct')
        end

        it 'returns a single topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Single)
        end

        it 'sets the seed on the topology' do
          expect(topology.seed).to eq('a')
        end
      end
    end

    context 'when provided a sharded option' do

      let(:topology) do
        described_class.initial([ 'a' ], monitoring, connect: :sharded)
      end

      it 'returns a sharded topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::Sharded)
      end

      context 'when the option is a String (due to YAML parsing)' do

        let(:topology) do
          described_class.initial([ 'a' ], monitoring, connect: 'sharded')
        end

        it 'returns a sharded topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Sharded)
        end
      end
    end

    context 'when provided no option' do

      context 'when a set name is in the options' do

        let(:topology) do
          described_class.initial([], monitoring, replica_set: 'testing')
        end

        it 'returns a replica set topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSet)
        end
      end

      context 'when no set name is in the options' do

        let(:topology) do
          described_class.initial([], monitoring, {})
        end

        it 'returns an unknown topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Unknown)
        end
      end

      context 'when provided a single mongos', if: single_mongos? do

        let(:topology) do
          described_class.initial(ADDRESSES, monitoring, TEST_OPTIONS)
        end

        it 'returns a sharded topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Sharded)
        end
      end

      context 'when provided a single replica set member', if: single_rs_member? do

        let(:topology) do
          described_class.initial(ADDRESSES, monitoring, TEST_OPTIONS)
        end

        it 'returns a single topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Single)
        end
      end
    end
  end
end
