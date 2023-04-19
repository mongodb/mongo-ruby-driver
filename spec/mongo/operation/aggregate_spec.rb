# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Aggregate do

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
      :db_name => SpecConfig.instance.test_db
    }
  end
  let(:op) { described_class.new(spec) }

  let(:context) { Mongo::Operation::Context.new }

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
          :db_name => SpecConfig.instance.test_db,
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
          op.execute(authorized_primary, context: context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
