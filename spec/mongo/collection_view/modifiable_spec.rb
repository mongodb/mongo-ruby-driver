require 'spec_helper'

describe Mongo::CollectionView::Modifiable do

  let(:collection) do
    authorized_client[TEST_COLL]
  end

  after do
    collection.find.remove
  end

  describe '#remove' do

    context 'when a selector was provided' do

      before do
        collection.insert([{ field: 'test1' }, { field: 'test2' }])
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

        it 'deletes the first document in the collection' do
          expect(response.n).to eq(1)
        end
      end
    end
  end

  describe '#update' do

    let(:collection) do
      authorized_client[TEST_COLL]
    end

    context 'when a selector was provided' do

      before do
        collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        collection.find(field: 'test1').update('$set'=> { field: 'testing' })
      end

      let(:updated) do
        collection.find(field: 'testing').first
      end

      it 'returns the number updated' do
        expect(response.n).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when no selector was provided' do

      before do
        collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        collection.find.update('$set'=> { field: 'testing' })
      end

      let(:updated) do
        collection.find
      end

      it 'returns the number updated' do
        expect(response.n).to eq(2)
      end

      it 'updates all the documents in the collection' do
        updated.each do |doc|
          expect(doc[:field]).to eq('testing')
        end
      end
    end

    context 'when limiting the number updated' do

      context 'when a selector was provided' do

        before do
          collection.insert([{ field: 'test1' }, { field: 'test1' }])
        end

        let!(:response) do
          collection.find(field: 'test1').limit(1).update('$set'=> { field: 'testing' })
        end

        let(:updated) do
          collection.find(field: 'testing').first
        end

        it 'updates the first matching document in the collection' do
          expect(response.n).to eq(1)
        end

        it 'updates the documents in the collection' do
          expect(updated[:field]).to eq('testing')
        end
      end

      context 'when no selector was provided' do

        before do
          collection.insert([{ field: 'test1' }, { field: 'test2' }])
        end

        let!(:response) do
          collection.find.limit(1).update('$set'=> { field: 'testing' })
        end

        let(:updated) do
          collection.find(field: 'testing').first
        end

        it 'updates the first document in the collection' do
          expect(response.n).to eq(1)
        end

        it 'updates the documents in the collection' do
          expect(updated[:field]).to eq('testing')
        end
      end
    end
  end
end
