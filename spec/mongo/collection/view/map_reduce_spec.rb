require 'spec_helper'

describe Mongo::Collection::View::MapReduce do

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

  let(:documents) do
    [
      { name: 'Berlin', population: 3000000 },
      { name: 'London', population: 9000000 }
    ]
  end

  let(:selector) do
    {}
  end

  let(:view_options) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, selector, view_options)
  end

  before do
    authorized_collection.insert_many(documents)
  end

  after do
    authorized_collection.find.remove_many
  end

  describe '#each' do

    let(:map_reduce) do
      described_class.new(view, map, reduce)
    end

    context 'when no options are provided' do

      it 'iterates over the documents in the result' do
        map_reduce.each do |document|
          expect(document[:value]).to_not be_nil
        end
      end
    end

    context 'when out is inline' do

    end

    context 'when out is a collection' do

      context 'when the option is to replace' do

      end

      context 'when the option is to merge' do

      end

      context 'when the option is to reduce' do

      end
    end

    context 'when the view has a selector' do

    end

    context 'when the view has a limit' do

    end

    context 'when the view has a sort' do

    end
  end

  pending '#finalize'
  pending '#js_mode'
  pending '#out'
  pending '#scope'
end
