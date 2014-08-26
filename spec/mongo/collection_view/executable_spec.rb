require 'spec_helper'

describe Mongo::CollectionView::Executable do

  after do
    authorized_collection.find.remove
  end

  describe '#count' do

    let(:documents) do
      (1..10).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert(documents)
    end

    after do
      authorized_collection.find.remove
    end

    context 'when a selector is provided' do

      let(:count) do
        authorized_collection.find(field: 'test1').count
      end

      it 'returns the count of matching documents' do
        expect(count).to eq(1)
      end
    end

    context 'when no selector is provided' do

      let(:count) do
        authorized_collection.find.count
      end

      it 'returns the count of matching documents' do
        expect(count).to eq(10)
      end
    end
  end

  describe '#remove' do

    context 'when a selector was provided' do

      before do
        authorized_collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        authorized_collection.find(field: 'test1').remove
      end

      it 'deletes the matching documents in the collection' do
        expect(response.n).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        authorized_collection.find.remove
      end

      it 'deletes all the documents in the collection' do
        expect(response.n).to eq(2)
      end
    end

    context 'when limiting the number removed' do

      context 'when a selector was provided' do

        before do
          authorized_collection.insert([{ field: 'test1' }, { field: 'test1' }])
        end

        let(:response) do
          authorized_collection.find(field: 'test1').limit(1).remove
        end

        it 'deletes the first matching document in the collection' do
          expect(response.n).to eq(1)
        end
      end

      context 'when no selector was provided' do

        before do
          authorized_collection.insert([{ field: 'test1' }, { field: 'test2' }])
        end

        let(:response) do
          authorized_collection.find.limit(1).remove
        end

        it 'deletes the first document in the collection' do
          expect(response.n).to eq(1)
        end
      end
    end
  end

  describe '#update' do

    context 'when a selector was provided' do

      before do
        authorized_collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        authorized_collection.find(field: 'test1').update('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
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
        authorized_collection.insert([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        authorized_collection.find.update('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find
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
          authorized_collection.insert([{ field: 'test1' }, { field: 'test1' }])
        end

        let!(:response) do
          authorized_collection.find(field: 'test1').limit(1).update('$set'=> { field: 'testing' })
        end

        let(:updated) do
          authorized_collection.find(field: 'testing').first
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
          authorized_collection.insert([{ field: 'test1' }, { field: 'test2' }])
        end

        let!(:response) do
          authorized_collection.find.limit(1).update('$set'=> { field: 'testing' })
        end

        let(:updated) do
          authorized_collection.find(field: 'testing').first
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
