require 'spec_helper'

describe Mongo::CollectionView::Modifiable do

  describe '#remove' do

    let(:collection) do
      authorized_client[TEST_COLL]
    end

    context 'when a selector was provided' do

      before do
        collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      after do
        collection.find.remove
      end

      let(:response) do
        collection.find(field: 'test1').remove
      end

      it 'deletes the matching documents in the collection' do
        expect(response.n).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        collection.find.remove
      end

      it 'deletes all the documents in the collection' do
        expect(response.n).to eq(2)
      end
    end

    context 'when limiting the number removed' do

      context 'when a selector was provided' do

        before do
          collection.insert([{ field: 'test1' }, { field: 'test1' }])
        end

        after do
          collection.find.remove
        end

        let(:response) do
          collection.find(field: 'test1').limit(1).remove
        end

        it 'deletes the first matching document in the collection' do
          expect(response.n).to eq(1)
        end
      end

      context 'when no selector was provided' do

        before do
          collection.insert([{ field: 'test1' }, { field: 'test2' }])
        end

        let(:response) do
          collection.find.limit(1).remove
        end

        after do
          collection.find.remove
        end

        it 'deletes the first document in the collection' do
          expect(response.n).to eq(1)
        end
      end
    end
  end
end
