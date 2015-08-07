require 'spec_helper'

describe Mongo::BulkWrite::OrderedCombiner do

  describe '#combine' do

    let(:combiner) do
      described_class.new(requests)
    end

    context 'when provided a series of insert one' do

      context 'when the documents are valid' do

        context 'when provided single documents' do

          let(:requests) do
            [{ insert_one: { _id: 0 }}, { insert_one: { _id: 1 }}]
          end

          it 'returns a single insert one' do
            expect(combiner.combine).to eq(
              [{ insert_one: [{ _id: 0 }, { _id: 1 }]}]
            )
          end
        end

        context 'when provided multiple documents' do

          let(:requests) do
            [{ insert_one: [{ _id: 0 }, { _id: 1 }]}]
          end

          it 'returns a single insert one' do
            expect(combiner.combine).to eq(
              [{ insert_one: [{ _id: 0 }, { _id: 1 }]}]
            )
          end
        end
      end

      context 'when a document is not valid' do

        let(:requests) do
          [{ insert_one: { _id: 0 }}, { insert_one: 'whoami' }]
        end

        it 'raises an exception' do
          expect {
            combiner.combine
          }.to raise_error(Mongo::Error::InvalidBulkOperation)
        end
      end
    end

    context 'when provided a mix of operations' do

      let(:requests) do
        [
          { insert_one: { _id: 0 }},
          { delete_one: { filter: { _id: 0 }}},
          { insert_one: [{ _id: 1 }]}
        ]
      end

      it 'returns an ordered grouping' do
        expect(combiner.combine).to eq(
          [
            { insert_one: [{ _id: 0 }]},
            { delete_one: [{ filter: { _id: 0 }}]},
            { insert_one: [{ _id: 1 }]}
          ]
        )
      end
    end
  end
end
