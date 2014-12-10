require 'spec_helper'

describe Mongo::BulkWrite do

  context 'ordered' do

    before do
      authorized_collection.find.remove_many
    end

    let(:bulk) do
      described_class.new(operations, options, authorized_collection)
    end

    let(:options) do
      { ordered: true }
    end

    context 'insert_one' do

      context 'when a document is provided' do

        let(:operations) do
          { insert_one: { name: 'test' } }
        end
  
        it 'returns nInserted of 1' do
          expect(
            bulk.execute['nInserted']
          ).to eq(1)
        end

        it 'only inserts that document' do
          bulk.execute
          expect(authorized_collection.find.first['name']).to eq('test')
        end
      end

      context 'when non-hash doc is provided' do

        let(:operations) do
          { insert_one: [] }
        end

        it 'raises an InvalidDoc exception' do
          expect do
            bulk.execute
          end.to raise_error(Mongo::BulkWrite::InvalidDoc)
        end
      end
    end

    context 'delete_one' do

      let(:docs) do
        [ { a: 1 }, { a: 1 } ]
      end

       let(:expected) do
        [ { 'a' => 1 } ]
      end

      before do
        authorized_collection.insert_many(docs)
      end

      after do
        authorized_collection.find.remove_many
      end

      let(:operations) do
        { delete_one: { a: 1 } }
      end

      context 'when no selector is specified' do
        let(:operations) do
          { delete_one: nil }
        end

        it 'raises an exception' do
          expect do
            bulk.execute
          end.to raise_exception(Mongo::BulkWrite::InvalidDoc)
        end
      end

      context 'when multiple documents match delete selector' do

        it 'reports nRemoved correctly' do
          expect(bulk.execute['nRemoved']).to eq(1)
        end

        it 'deletes only matching documents' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end

    context 'delete_many' do

      let(:docs) do
        [ { a: 1 }, { a: 1 } ]
      end

      before do
        authorized_collection.insert_many(docs)
      end

      after do
        authorized_collection.find.remove_many
      end

      let(:operations) do
        { delete_many: { a: 1 } }
      end

      context 'when no selector is specified' do

        let(:operations) do
          { delete_many: nil }
        end

        it 'raises an exception' do
          expect do
            bulk.execute
          end.to raise_exception(Mongo::BulkWrite::InvalidDoc)
        end
      end

      context 'when a selector is specified' do

        context 'when multiple documents match delete selector' do

          it 'reports nRemoved correctly' do
            expect(bulk.execute['nRemoved']).to eq(2)
          end

          it 'deletes all matching documents' do
            bulk.execute
            expect(authorized_collection.find.to_a).to be_empty
          end
        end

        context 'when only one document matches delete selector' do

          let(:docs) do
            [ { a: 1 }, { a: 2 } ]
          end

          let(:expected) do
            [ { 'a' => 2 } ]
          end

          it 'reports nRemoved correctly' do
            expect(bulk.execute['nRemoved']).to eq(1)
          end

          it 'deletes all matching documents' do
            bulk.execute
            expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
          end
        end
      end
    end
  end
end
