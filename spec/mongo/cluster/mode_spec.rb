require 'spec_helper'

describe Mongo::Cluster::Mode do

  describe '.get' do

    context 'when provided a replica set option' do

      let(:mode) do
        described_class.get(mode: :replica_set)
      end

      it 'returns a replica set mode' do
        expect(mode).to eq(Mongo::Cluster::Mode::ReplicaSet)
      end
    end

    context 'when provided a standalone option' do

      let(:mode) do
        described_class.get(mode: :standalone)
      end

      it 'returns a standalone mode' do
        expect(mode).to eq(Mongo::Cluster::Mode::Standalone)
      end
    end

    context 'when provided a sharded option' do

      let(:mode) do
        described_class.get(mode: :sharded)
      end

      it 'returns a sharded mode' do
        expect(mode).to eq(Mongo::Cluster::Mode::Sharded)
      end
    end

    context 'when provided no option' do

      context 'when a set name is in the options' do

        let(:mode) do
          described_class.get(set_name: 'testing')
        end

        it 'returns a replica set mode' do
          expect(mode).to eq(Mongo::Cluster::Mode::ReplicaSet)
        end
      end

      context 'when no set name is in the options' do

        let(:mode) do
          described_class.get({})
        end

        it 'returns a standalone mode' do
          expect(mode).to eq(Mongo::Cluster::Mode::Standalone)
        end
      end
    end
  end
end
