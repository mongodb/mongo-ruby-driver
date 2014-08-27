require 'spec_helper'

describe Mongo::Collection do

  describe '#==' do

    let(:database) { Mongo::Database.new(authorized_client, :test) }
    let(:collection) { described_class.new(database, :users) }

    context 'when the names are the same' do

      context 'when the databases are the same' do

        let(:other) { described_class.new(database, :users) }

        it 'returns true' do
          expect(collection).to eq(other)
        end
      end

      context 'when the databases are not the same' do

        let(:other_db) { Mongo::Database.new(authorized_client, :testing) }
        let(:other) { described_class.new(other_db, :users) }

        it 'returns false' do
          expect(collection).to_not eq(other)
        end
      end
    end

    context 'when the names are not the same' do

      let(:other) { described_class.new(database, :sounds) }

      it 'returns false' do
        expect(collection).to_not eq(other)
      end
    end

    context 'when the object is not a collection' do

      it 'returns false' do
        expect(collection).to_not eq('test')
      end
    end
  end

  describe '#find' do

    before do
      authorized_collection.drop_indexes
    end

    context 'when provided a selector' do

      let(:view) do
        authorized_collection.find(name: 1)
      end

      it 'returns a authorized_collection view for the selector' do
        expect(view.selector).to eq(name: 1)
      end
    end

    context 'when provided no selector' do

      let(:view) do
        authorized_collection.find
      end

      it 'returns a authorized_collection view with an empty selector' do
        expect(view.selector).to be_empty
      end
    end

    context 'when iterating the authorized_collection view' do

      before do
        authorized_collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      after do
        authorized_collection.find.remove
      end

      let(:view) do
        authorized_collection.find
      end

      it 'iterates over the documents' do
        view.each do |document|
          expect(document).to_not be_nil
        end
      end
    end
  end

  describe '#insert' do

    let(:collection) do
      authorized_client[TEST_COLL]
    end

    after do
      Mongo::Operation::Write::Delete.new({
        deletes: [{ q: {}, limit: -1 }],
        db_name: TEST_DB,
        coll_name: TEST_COLL,
        write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
      }).execute(authorized_primary.context)
    end

    context 'when providing a single document' do

      let(:result) do
        collection.insert({ name: 'testing' })
      end

      it 'inserts the document into the collection' do
        expect(result.n).to eq(1)
      end
    end

    context 'when providing multiple documents' do

      let(:result) do
        collection.insert([{ name: 'test1' }, { name: 'test2' }])
      end

      it 'inserts the documents into the collection' do
        expect(result.n).to eq(2)
      end
    end
  end
end
