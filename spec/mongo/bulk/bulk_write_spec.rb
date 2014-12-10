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
  end

  context 'unordered' do

    before do
      authorized_collection.find.remove_many
    end

    let(:bulk) do
      described_class.new(operations, options, authorized_collection)
    end

    let(:options) do
      { ordered: false }
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
  end
end
