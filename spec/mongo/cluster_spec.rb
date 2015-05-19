require 'spec_helper'

describe Mongo::Cluster do

  let(:cluster) do
    described_class.new(ADDRESSES, TEST_OPTIONS)
  end

  describe '#==' do

    context 'when the other is a cluster' do

      context 'when the addresses are the same' do

        context 'when the options are the same' do

          let(:other) do
            described_class.new(ADDRESSES, TEST_OPTIONS)
          end

          it 'returns true' do
            expect(cluster).to eq(other)
          end
        end

        context 'when the options are not the same' do

          let(:other) do
            described_class.new([ '127.0.0.1:27017' ], TEST_OPTIONS.merge(:replica_set => 'test'))
          end

          it 'returns false' do
            expect(cluster).to_not eq(other)
          end
        end
      end

      context 'when the addresses are not the same' do

        let(:other) do
          described_class.new([ '127.0.0.1:27018' ], TEST_OPTIONS)
        end

        it 'returns false' do
          expect(cluster).to_not eq(other)
        end
      end
    end

    context 'when the other is not a cluster' do

      it 'returns false' do
        expect(cluster).to_not eq('test')
      end
    end
  end

  describe '#inspect' do

    let(:preference) do
      Mongo::ServerSelector.get
    end

    it 'displays the cluster seeds and topology' do
      expect(cluster.inspect).to include('topology')
      expect(cluster.inspect).to include('servers')
    end
  end

  describe '#replica_set_name' do

    let(:preference) do
      Mongo::ServerSelector.get
    end

    context 'when the option is provided' do

      let(:cluster) do
        described_class.new([ '127.0.0.1:27017' ], TEST_OPTIONS.merge(:replica_set => 'testing'))
      end

      it 'returns the name' do
        expect(cluster.replica_set_name).to eq('testing')
      end
    end

    context 'when the option is not provided' do

      let(:cluster) do
        described_class.new([ '127.0.0.1:27017' ], TEST_OPTIONS)
      end

      it 'returns nil' do
        expect(cluster.replica_set_name).to be_nil
      end
    end
  end

  describe '#scan!' do

    let(:preference) do
      Mongo::ServerSelector.get
    end

    let(:known_servers) do
      cluster.instance_variable_get(:@servers)
    end

    before do
      expect(known_servers.first).to receive(:scan!).and_call_original
    end

    it 'returns true' do
      expect(cluster.scan!).to be true
    end
  end

  describe '#servers' do

    context 'when topology is single', if: single_seed? do

      context 'when the server is a mongos', if: single_mongos?  do

        it 'returns the mongos' do
          expect(cluster.servers.size).to eq(1)
        end
      end

      context 'when the server is a replica set member', if: single_rs_member? do

        it 'returns the replica set member' do
          expect(cluster.servers.size).to eq(1)
        end
      end
    end

    context 'when the cluster has no servers' do

      let(:servers) do
        [nil]
      end

      before do
        cluster.instance_variable_set(:@servers, servers)
        cluster.instance_variable_set(:@topology, topology)
      end

      context 'when topology is Single' do

        let(:topology) do
          Mongo::Cluster::Topology::Single.new({})
        end

        it 'returns an empty array' do
          expect(cluster.servers).to eq([])
        end
      end

      context 'when topology is ReplicaSet' do

        let(:topology) do
          Mongo::Cluster::Topology::ReplicaSet.new({})
        end

        it 'returns an empty array' do
          expect(cluster.servers).to eq([])
        end
      end

      context 'when topology is Sharded' do

        let(:topology) do
          Mongo::Cluster::Topology::Sharded.new({})
        end

        it 'returns an empty array' do
          expect(cluster.servers).to eq([])
        end
      end

      context 'when topology is Unknown' do

        let(:topology) do
          Mongo::Cluster::Topology::Unknown.new({})
        end

        it 'returns an empty array' do
          expect(cluster.servers).to eq([])
        end
      end
    end
  end

  describe '#add' do

    context 'when topology is Single' do

      let(:topology) do
        Mongo::Cluster::Topology::Single.new({})
      end

      before do
        cluster.add('a')
      end

      it 'does not add discovered servers to the cluster' do
        expect(cluster.servers[0].address.seed).to_not eq('a')
      end
    end
  end

  describe '#disconnect!' do

    let(:known_servers) do
      cluster.instance_variable_get(:@servers)
    end

    before do
      known_servers.each do |server|
        expect(server).to receive(:disconnect!).and_call_original
      end
    end

    it 'disconnects each server and returns true' do
      expect(cluster.disconnect!).to be(true)
    end
  end

  describe '#reconnect!' do

    before do
      cluster.servers.each do |server|
        expect(server).to receive(:reconnect!).and_call_original
      end
    end

    it 'reconnects each server and returns true' do
      expect(cluster.reconnect!).to be(true)
    end
  end
end
