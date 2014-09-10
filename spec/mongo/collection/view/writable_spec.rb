require 'spec_helper'

describe Mongo::Collection::View::Writable do

  let(:selector) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, selector, options)
  end

  after do
    authorized_collection.find.remove_many
  end

  describe '#remove_many' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.remove_many
      end

      it 'deletes the matching documents in the collection' do
        expect(response.written_count).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.remove_many
      end

      it 'deletes all the documents in the collection' do
        expect(response.written_count).to eq(2)
      end
    end
  end

  describe '#remove_one' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([
          { field: 'test1' },
          { field: 'test1' },
          { field: 'test1' }
        ])
      end

      let(:response) do
        view.remove_one
      end

      it 'deletes the first matching document in the collection' do
        expect(response.written_count).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.remove_one
      end

      it 'deletes the first document in the collection' do
        expect(response.written_count).to eq(1)
      end
    end
  end

  describe '#replace_one' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let!(:response) do
        view.replace_one({ field: 'testing' })
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
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.replace_one({ field: 'testing' })
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

  describe '#update_many' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.update_many('$set'=> { field: 'testing' })
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
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.update_many('$set'=> { field: 'testing' })
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
  end

  describe '#update_one' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let!(:response) do
        view.update_one('$set'=> { field: 'testing' })
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
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.update_one('$set'=> { field: 'testing' })
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
