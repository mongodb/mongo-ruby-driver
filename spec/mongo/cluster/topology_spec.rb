require 'lite_spec_helper'

describe Mongo::Cluster::Topology do

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:cluster) { Mongo::Cluster.new(['a'], Mongo::Monitoring.new(monitoring: false)) }

  describe '.initial' do

    context 'when provided a replica set option' do

      let(:topology) do
        described_class.initial(cluster, monitoring, connect: :replica_set, replica_set_name: 'foo')
      end

      it 'returns a replica set topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
      end

      context 'when the option is a String (due to YAML parsing)' do

        let(:topology) do
          described_class.initial(cluster, monitoring, connect: 'replica_set', replica_set_name: 'foo')
        end

        it 'returns a replica set topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
        end
      end
    end

    context 'when provided a single option' do

      let(:topology) do
        described_class.initial(cluster, monitoring, connect: :direct)
      end

      it 'returns a single topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::Single)
      end

      it 'sets the seed on the topology' do
        expect(topology.addresses).to eq(['a'])
      end

      context 'when the option is a String (due to YAML parsing)' do

        let(:topology) do
          described_class.initial(cluster, monitoring, connect: 'direct')
        end

        it 'returns a single topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Single)
        end

        it 'sets the seed on the topology' do
          expect(topology.addresses).to eq(['a'])
        end
      end
    end

    context 'when provided a sharded option' do

      let(:topology) do
        described_class.initial(cluster, monitoring, connect: :sharded)
      end

      it 'returns a sharded topology' do
        expect(topology).to be_a(Mongo::Cluster::Topology::Sharded)
      end

      context 'when the option is a String (due to YAML parsing)' do

        let(:topology) do
          described_class.initial(cluster, monitoring, connect: 'sharded')
        end

        it 'returns a sharded topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Sharded)
        end
      end
    end

    context 'when provided no option' do

      context 'when a set name is in the options' do

        let(:topology) do
          described_class.initial(cluster, monitoring, replica_set_name: 'testing')
        end

        it 'returns a replica set topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
        end
      end

      context 'when no set name is in the options' do

        let(:topology) do
          described_class.initial(cluster, monitoring, {})
        end

        it 'returns an unknown topology' do
          expect(topology).to be_a(Mongo::Cluster::Topology::Unknown)
        end
      end
    end
  end
end
