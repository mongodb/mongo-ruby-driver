require 'spec_helper'

describe Mongo::Operation::Commands::Aggregate do

  let(:options) do
    {}
  end

  let(:selector) do
    { :aggregate => TEST_COLL,
      :pipeline => [],
    }
  end
  let(:spec) do
    { :selector => selector,
      :options => options,
      :db_name => TEST_DB
    }
  end
  let(:op) { described_class.new(spec) }


  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to be(spec)
      end
    end
  end

  describe '#==' do

    context ' when two ops have different specs' do
      let(:other_selector) do
        { :aggregate => 'another_test_coll',
          :pipeline => [],
        }
      end
      let(:other_spec) do
        { :selector => other_selector,
          :options => options,
          :db_name => TEST_DB,
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#execute' do

    context 'when the aggregation fails' do

      let(:selector) do
        { :aggregate => TEST_COLL,
          :pipeline => [{ '$invalid' => 'operator' }],
        }
      end

      it 'raises an exception' do
        expect {
          op.execute(authorized_primary)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  context 'when the server is a mongos', if: sessions_enabled? do

    let(:cluster) do
      authorized_primary.cluster.dup
    end

    let(:server) do
      double('server').tap do |s|
        allow(s).to receive(:features).and_return(authorized_primary.features)
        allow(s).to receive(:cluster).and_return(cluster)
        allow(s).to receive(:mongos?).and_return(true)
        allow(s).to receive(:cluster_time).and_return(BSON::Timestamp.new(5, 20))
      end
    end

    it 'adds clusterTime to the selector' do
      expect(op.send(:message, server).selector[:'$clusterTime']).to eq(BSON::Timestamp.new(5, 20))
    end

    context 'when a clusterTime is reported in the response' do

      let(:result) do
        double('result').tap do |result|
          allow(result).to receive(:cluster_time).and_return(BSON::Timestamp.new(5, 30))
          allow(result).to receive(:validate!).and_return(true)
        end
      end

      before do
        allow(described_class::Result).to receive(:new).and_return(result)
        op.execute(authorized_primary)
      end

      it 'updates the cluster with the cluster time reported in the response' do
        expect(cluster.instance_variable_get(:@cluster_time)).to eq(BSON::Timestamp.new(5, 30))
      end
    end
  end
end
