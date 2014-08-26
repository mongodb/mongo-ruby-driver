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

  describe '#distinct' do

    context 'when a selector is provided' do

      let(:documents) do
        (1..3).map{ |i| { field: "test" }}
      end

      before do
        authorized_collection.insert(documents)
      end

      after do
        authorized_collection.find.remove
      end

      context 'when the field is a symbol' do

        let(:distinct) do
          authorized_collection.find(field: 'test').distinct(:field)
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test' ])
        end
      end

      context 'when the field is a string' do

        let(:distinct) do
          authorized_collection.find(field: 'test').distinct('field')
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test' ])
        end
      end

      context 'when the field is nil' do

        let(:distinct) do
          authorized_collection.find(field: 'test').distinct(nil)
        end

        it 'returns an empty array' do
          expect(distinct).to be_empty
        end
      end
    end

    context 'when no selector is provided' do

      let(:documents) do
        (1..3).map{ |i| { field: "test#{i}" }}
      end

      before do
        authorized_collection.insert(documents)
      end

      after do
        authorized_collection.find.remove
      end

      context 'when the field is a symbol' do

        let(:distinct) do
          authorized_collection.find.distinct(:field)
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when the field is a string' do

        let(:distinct) do
          authorized_collection.find.distinct('field')
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when the field is nil' do

        let(:distinct) do
          authorized_collection.find.distinct(nil)
        end

        it 'returns an empty array' do
          expect(distinct).to be_empty
        end
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
