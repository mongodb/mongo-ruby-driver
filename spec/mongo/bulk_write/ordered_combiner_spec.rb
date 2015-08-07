require 'spec_helper'

describe Mongo::BulkWrite::OrderedCombiner do

  describe '#combine' do

    let(:combiner) do
      described_class.new(requests)
    end

    context 'when provided a series of insert one' do

      let(:requests) do
        [{ insert_one: { _id: 0 }}, { insert_one: { _id: 1 }}]
      end

      it 'returns a single insert many' do
        expect(combiner.combine).to eq(
          [{ insert_many: [{ _id: 0 }, { _id: 1 }]}]
        )
      end
    end

    context 'when provided a series of insert many' do

      let(:requests) do
        [{ insert_many: [{ _id: 0 }]}, { insert_many: [{ _id: 1 }]}]
      end

      it 'returns a single insert many' do
        expect(combiner.combine).to eq(
          [{ insert_many: [{ _id: 0 }, { _id: 1 }]}]
        )
      end
    end

    context 'when provided a mix of operations' do

      let(:requests) do
        [
          { insert_one: { _id: 0 }},
          { delete_one: { _id: 0 }},
          { insert_many: [{ _id: 1 }]}
        ]
      end

      it 'returns an ordered grouping' do
        expect(combiner.combine).to eq(
          [
            { insert_many: [{ _id: 0 }]},
            { delete_one: [{ _id: 0 }]},
            { insert_many: [{ _id: 1 }]}
          ]
        )
      end
    end
  end
end
