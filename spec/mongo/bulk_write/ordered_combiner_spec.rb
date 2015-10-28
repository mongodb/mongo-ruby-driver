require 'spec_helper'

describe Mongo::BulkWrite::OrderedCombiner do

  describe '#combine' do

    let(:combiner) do
      described_class.new(requests)
    end

    context 'when provided a series of delete one' do

      context 'when the documents are valid' do

        let(:requests) do
          [
            { delete_one: { filter: { _id: 0 }}},
            { delete_one: { filter: { _id: 1 }}}
          ]
        end

        it 'returns a single delete one' do
          expect(combiner.combine).to eq(
            [
              {
                delete_one: [
                  { 'q' => { _id: 0 }, 'limit' => 1 },
                  { 'q' => { _id: 1 }, 'limit' => 1 }
                ]
              }
            ]
          )
        end
      end

      context 'when a document is not valid' do

        let(:requests) do
          [
            { delete_one: { filter: { _id: 0 }}},
            { delete_one: 'whoami' }
          ]
        end

        it 'raises an exception' do
          expect {
            combiner.combine
          }.to raise_error(Mongo::Error::InvalidBulkOperation)
        end
      end
    end

    context 'when provided a series of delete many' do

      context 'when the documents are valid' do

        let(:requests) do
          [
            { delete_many: { filter: { _id: 0 }}},
            { delete_many: { filter: { _id: 1 }}}
          ]
        end

        it 'returns a single delete many' do
          expect(combiner.combine).to eq(
            [
              {
                delete_many: [
                  { 'q' => { _id: 0 }, 'limit' => 0 },
                  { 'q' => { _id: 1 }, 'limit' => 0 }
                ]
              }
            ]
          )
        end
      end

      context 'when a document is not valid' do

        let(:requests) do
          [
            { delete_many: { filter: { _id: 0 }}},
            { delete_many: 'whoami' }
          ]
        end

        it 'raises an exception' do
          expect {
            combiner.combine
          }.to raise_error(Mongo::Error::InvalidBulkOperation)
        end
      end
    end

    context 'when provided a series of insert one' do

      context 'when providing only one operation' do

        let(:requests) do
          [{ insert_one: { _id: 0 }}]
        end

        it 'returns a single insert one' do
          expect(combiner.combine).to eq(
            [{ insert_one: [{ _id: 0 }]}]
          )
        end
      end

      context 'when the documents are valid' do

        let(:requests) do
          [{ insert_one: { _id: 0 }}, { insert_one: { _id: 1 }}]
        end

        it 'returns a single insert one' do
          expect(combiner.combine).to eq(
            [{ insert_one: [{ _id: 0 }, { _id: 1 }]}]
          )
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

    context 'when provided a series of replace one' do

      context 'when the documents are valid' do

        let(:requests) do
          [
            { replace_one: { filter: { _id: 0 }, replacement: { name: 'test' }}},
            { replace_one: { filter: { _id: 1 }, replacement: { name: 'test' }}}
          ]
        end

        it 'returns a single replace one' do
          expect(combiner.combine).to eq(
            [
              {
                replace_one: [
                  { 'q' => { _id: 0 }, 'u' => { name: 'test' }, 'multi' => false, 'upsert' => false },
                  { 'q' => { _id: 1 }, 'u' => { name: 'test' }, 'multi' => false, 'upsert' => false }
                ]
              }
            ]
          )
        end
      end

      context 'when a document is not valid' do

        let(:requests) do
          [
            { replace_one: { filter: { _id: 0 }, replacement: { name: 'test' }}},
            { replace_one: 'whoami' }
          ]
        end

        it 'raises an exception' do
          expect {
            combiner.combine
          }.to raise_error(Mongo::Error::InvalidBulkOperation)
        end
      end
    end

    context 'when provided a series of update one' do

      context 'when the documents are valid' do

        let(:requests) do
          [
            { update_one: { filter: { _id: 0 }, update: { '$set' => { name: 'test' }}}},
            { update_one: { filter: { _id: 1 }, update: { '$set' => { name: 'test' }}}}
          ]
        end

        it 'returns a single update one' do
          expect(combiner.combine).to eq(
            [
              {
                update_one: [
                  { 'q' => { _id: 0 }, 'u' => { '$set' => { name: 'test' }}, 'multi' => false, 'upsert' => false },
                  { 'q' => { _id: 1 }, 'u' => { '$set' => { name: 'test' }}, 'multi' => false, 'upsert' => false }
                ]
              }
            ]
          )
        end
      end

      context 'when a document is not valid' do

        let(:requests) do
          [
            { update_one: { filter: { _id: 0 }, update: { '$set' => { name: 'test' }}}},
            { update_one: 'whoami' }
          ]
        end

        it 'raises an exception' do
          expect {
            combiner.combine
          }.to raise_error(Mongo::Error::InvalidBulkOperation)
        end
      end
    end

    context 'when provided a series of update many ops' do

      context 'when the documents are valid' do

        let(:requests) do
          [
            { update_many: { filter: { _id: 0 }, update: { '$set' => { name: 'test' }}}},
            { update_many: { filter: { _id: 1 }, update: { '$set' => { name: 'test' }}}}
          ]
        end

        it 'returns a single update many' do
          expect(combiner.combine).to eq(
            [
              {
                update_many: [
                  { 'q' => { _id: 0 }, 'u' => { '$set' => { name: 'test' }}, 'multi' => true, 'upsert' => false },
                  { 'q' => { _id: 1 }, 'u' => { '$set' => { name: 'test' }}, 'multi' => true, 'upsert' => false }
                ]
              }
            ]
          )
        end
      end

      context 'when a document is not valid' do

        let(:requests) do
          [
            { update_many: { filter: { _id: 0 }, update: { '$set' => { name: 'test' }}}},
            { update_many: 'whoami' }
          ]
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
          { insert_one: { _id: 1 }}
        ]
      end

      it 'returns an ordered grouping' do
        expect(combiner.combine).to eq(
          [
            { insert_one: [{ _id: 0 }]},
            { delete_one: [{ 'q' => { _id: 0 }, 'limit' => 1 }]},
            { insert_one: [{ _id: 1 }]}
          ]
        )
      end
    end
  end
end
