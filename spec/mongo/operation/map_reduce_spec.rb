require 'spec_helper'

describe Mongo::Operation::MapReduce do
  include_context 'operation'

  let(:options) { {} }
  let(:selector) do
    { :mapreduce => 'test_coll',
      :map       => '',
      :reduce    => '',
    }
  end
  let(:spec) do
    { :selector => selector,
      :options     => options,
      :db_name  => db_name
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
        { :mapreduce => 'other_test_coll',
          :map => '',
          :reduce => '',
        }
      end
      let(:other_spec) do
        { :selector => other_selector,
          :options => {},
          :db_name => db_name,
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#merge' do

    let(:other_op) { described_class.new(spec) }

    it 'is not allowed' do
      expect{ op.merge(other_op) }.to raise_exception
    end
  end

  describe '#merge!' do

    let(:other_op) { described_class.new(spec) }

    it 'is not allowed' do
      expect{ op.merge!(other_op) }.to raise_exception
    end
  end

  describe '#execute' do

  end
end
