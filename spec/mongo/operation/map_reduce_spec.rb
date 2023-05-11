# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::MapReduce do
  require_no_required_api_version

  let(:context) { Mongo::Operation::Context.new }

  let(:map) do
  %Q{
  function() {
    emit(this.name, { population: this.population });
  }}
  end

  let(:reduce) do
    %Q{
    function(key, values) {
      var result = { population: 0 };
      values.forEach(function(value) {
        result.population += value.population;
      });
      return result;
    }}
  end

  let(:options) do
    {}
  end

  let(:selector) do
    { :mapreduce => TEST_COLL,
      :map => map,
      :reduce => reduce,
      :query => {},
      :out => { inline: 1 }
    }
  end

  let(:spec) do
    { :selector => selector,
      :options  => options,
      :db_name  => SpecConfig.instance.test_db
    }
  end

  let(:op) do
    described_class.new(spec)
  end

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

    let(:documents) do
      [
        { name: 'Berlin', population: 3000000 },
        { name: 'London', population: 9000000 }
      ]
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.delete_many
    end

    context 'when the map/reduce succeeds' do

      let(:response) do
        op.execute(authorized_primary, context: context)
      end

      it 'returns the response' do
        expect(response).to be_successful
      end
    end

    context 'when the map/reduce fails' do

      let(:selector) do
        { :mapreduce => TEST_COLL,
          :map => map,
          :reduce => reduce,
          :query => {}
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
