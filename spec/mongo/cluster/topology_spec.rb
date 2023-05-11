# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Cluster::Topology do

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:cluster) { Mongo::Cluster.new(['a'], Mongo::Monitoring.new, monitoring_io: false) }

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

  describe '#logical_session_timeout' do
    require_no_linting

    let(:listeners) do
      Mongo::Event::Listeners.new
    end

    let(:monitoring) do
      Mongo::Monitoring.new(monitoring: false)
    end

    let(:server_one) do
      Mongo::Server.new(Mongo::Address.new('a:27017'),
        cluster, monitoring, listeners, monitoring_io: false)
    end

    let(:server_two) do
      Mongo::Server.new(Mongo::Address.new('b:27017'),
        cluster, monitoring, listeners, monitoring_io: false)
    end

    let(:servers) do
      [ server_one, server_two ]
    end

    let(:topology) do
      Mongo::Cluster::Topology::Sharded.new({}, monitoring, cluster)
    end

    before do
      expect(cluster).to receive(:servers_list).and_return(servers)
    end

    context 'when servers are data bearing' do
      before do
        expect(server_one.description).to receive(:primary?).and_return(true)
        allow(server_two.description).to receive(:primary?).and_return(true)
      end

      context 'when one server has a nil logical session timeout value' do

        before do
          expect(server_one.description).to receive(:logical_session_timeout).and_return(7)
          expect(server_two.description).to receive(:logical_session_timeout).and_return(nil)
        end

        it 'returns nil' do
          expect(topology.logical_session_timeout).to be(nil)
        end
      end

      context 'when all servers have a logical session timeout value' do

        before do
          expect(server_one.description).to receive(:logical_session_timeout).and_return(7)
          expect(server_two.description).to receive(:logical_session_timeout).and_return(3)
        end

        it 'returns the minimum' do
          expect(topology.logical_session_timeout).to be(3)
        end
      end

      context 'when no servers have a logical session timeout value' do

        before do
          expect(server_one.description).to receive(:logical_session_timeout).and_return(nil)
          allow(server_two.description).to receive(:logical_session_timeout).and_return(nil)
        end

        it 'returns nil' do
          expect(topology.logical_session_timeout).to be(nil)
        end
      end
    end

    context 'when servers are not data bearing' do
      before do
        expect(server_one).to be_unknown
        expect(server_two).to be_unknown
      end

      context 'when all servers have a logical session timeout value' do

        before do
          expect(server_one).not_to receive(:logical_session_timeout)
          expect(server_two).not_to receive(:logical_session_timeout)
        end

        it 'returns nil' do
          expect(topology.logical_session_timeout).to be nil
        end
      end
    end
  end
end
