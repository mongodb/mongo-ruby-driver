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

  describe '#find_one_and_delete' do

    before do
      authorized_collection.insert_many([{ field: 'test1' }])
    end

    context 'when a matching document is found' do

      let(:selector) do
        { field: 'test1' }
      end

      context 'when no options are provided' do

        let!(:document) do
          view.find_one_and_delete
        end

        it 'deletes the document from the database' do
          expect(view.to_a).to be_empty
        end

        it 'returns the document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when a projection is provided' do

        let!(:document) do
          view.projection(_id: 1).find_one_and_delete
        end

        it 'deletes the document from the database' do
          expect(view.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let!(:document) do
          view.sort(field: 1).find_one_and_delete
        end

        it 'deletes the document from the database' do
          expect(view.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to eq('test1')
        end
      end
    end

    context 'when no matching document is found' do

      let(:selector) do
        { field: 'test5' }
      end

      let!(:document) do
        view.find_one_and_delete
      end

      it 'returns nil' do
        expect(document).to be_nil
      end
    end
  end

  describe '#find_one_and_update' do

    before do
      authorized_collection.insert_many([{ field: 'test1' }])
    end

    context 'when a matching document is found' do

      let(:selector) do
        { field: 'test1' }
      end

      context 'when no options are provided' do

        let(:document) do
          view.find_one_and_update({ '$set' => { field: 'testing' }})
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when return_document options are provided' do

        let(:document) do
          view.find_one_and_update({ '$set' => { field: 'testing' }}, :return_document => :after)
        end

        it 'returns the original document' do
          expect(document['field']).to eq('testing')
        end
      end

      context 'when a projection is provided' do

        let(:document) do
          view.projection(_id: 1).find_one_and_update({ '$set' => { field: 'testing' }})
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let(:document) do
          view.sort(field: 1).find_one_and_update({ '$set' => { field: 'testing' }})
        end

        it 'returns the original document' do
          expect(document['field']).to eq('test1')
        end
      end
    end

    context 'when no matching document is found' do

      let(:selector) do
        { field: 'test5' }
      end

      let(:document) do
        view.find_one_and_update({ '$set' => { field: 'testing' }})
      end

      it 'returns nil' do
        expect(document).to be_nil
      end
    end
  end

  pending '#find_one_and_replace'

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
        expect(response.written_count).to eq(1)
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
        expect(response.written_count).to eq(1)
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
        expect(response.written_count).to eq(1)
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
        expect(response.written_count).to eq(2)
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
        expect(response.written_count).to eq(1)
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
        expect(response.written_count).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end
  end
end
