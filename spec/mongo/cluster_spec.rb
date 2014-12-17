require 'spec_helper'

describe Mongo::Cluster do

  describe '#==' do

    let(:preference) do
      Mongo::ServerPreference.get
    end

    let(:cluster) do
      described_class.new([ '127.0.0.1:27017' ], preference)
    end

    context 'when the other is a cluster' do

      context 'when the addresses are the same' do

        context 'when the options are the same' do

          let(:other) do
            described_class.new([ '127.0.0.1:27017' ], preference)
          end

          it 'returns true' do
            expect(cluster).to eq(other)
          end
        end

        context 'when the options are not the same' do

          let(:other) do
            described_class.new([ '127.0.0.1:27017' ], preference, :replica_set => 'test')
          end

          it 'returns false' do
            expect(cluster).to_not eq(other)
          end
        end
      end

      context 'when the addresses are not the same' do

        let(:other) do
          described_class.new([ '127.0.0.1:27018' ], preference)
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
      Mongo::ServerPreference.get
    end

    let(:cluster) do
      described_class.new([ '127.0.0.1:27017' ], preference)
    end

    it 'displays the cluster seeds and topology' do
      expect(cluster.inspect).to include('topology')
      expect(cluster.inspect).to include('servers')
    end
  end

  describe '#replica_set_name' do

    let(:preference) do
      Mongo::ServerPreference.get
    end

    let(:cluster) do
      described_class.new([ '127.0.0.1:27017' ], preference, :replica_set => 'testing')
    end

    context 'when the option is provided' do

      let(:cluster) do
        described_class.new([ '127.0.0.1:27017' ], preference, :replica_set => 'testing')
      end

      it 'returns the name' do
        expect(cluster.replica_set_name).to eq('testing')
      end
    end

    context 'when the option is not provided' do

      let(:cluster) do
        described_class.new([ '127.0.0.1:27017' ], preference)
      end

      it 'returns nil' do
        expect(cluster.replica_set_name).to be_nil
      end
    end
  end
end
