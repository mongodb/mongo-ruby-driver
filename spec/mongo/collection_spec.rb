require 'spec_helper'

describe Mongo::Collection do

  describe '#==' do

    let(:database) do
      Mongo::Database.new(authorized_client, :test)
    end

    let(:collection) do
      described_class.new(database, :users)
    end

    context 'when the names are the same' do

      context 'when the databases are the same' do

        let(:other) do
          described_class.new(database, :users)
        end

        it 'returns true' do
          expect(collection).to eq(other)
        end
      end

      context 'when the databases are not the same' do

        let(:other_db) do
          Mongo::Database.new(authorized_client, :testing)
        end

        let(:other) do
          described_class.new(other_db, :users)
        end

        it 'returns false' do
          expect(collection).to_not eq(other)
        end
      end

      context 'when the options are the same' do

        let(:other) do
          described_class.new(database, :users)
        end

        it 'returns true' do
          expect(collection).to eq(other)
        end
      end

      context 'when the options are not the same' do

        let(:other) do
          described_class.new(database, :users, :capped => true)
        end

        it 'returns false' do
          expect(collection).to_not eq(other)
        end
      end
    end

    context 'when the names are not the same' do

      let(:other) do
        described_class.new(database, :sounds)
      end

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

  describe '#capped?' do

    let(:database) do
      authorized_client.database
    end

    context 'when the collection is capped' do

      let(:collection) do
        described_class.new(database, :specs, :capped => true, :size => 1024)
      end

      before do
        collection.create
      end

      after do
        collection.drop
      end

      it 'returns true' do
        expect(collection).to be_capped
      end
    end

    context 'when the collection is not capped' do

      let(:collection) do
        described_class.new(database, :specs)
      end

      before do
        collection.create
      end

      after do
        collection.drop
      end

      it 'returns false' do
        expect(collection).to_not be_capped
      end
    end
  end

  describe '#create' do

    let(:database) do
      authorized_client.database
    end

    context 'when the collection has no options' do

      let(:collection) do
        described_class.new(database, :specs)
      end

      let!(:response) do
        collection.create
      end

      after do
        collection.drop
      end

      it 'executes the command' do
        expect(response).to be_successful
      end

      it 'creates the collection in the database' do
        expect(database.collection_names).to include('specs')
      end
    end

    context 'when the collection has options' do

      context 'when the collection is capped' do

        shared_examples 'a capped collection command' do

          let!(:response) do
            collection.create
          end

          after do
            collection.drop
          end

          it 'executes the command' do
            expect(response).to be_successful
          end

          it 'sets the collection as capped' do
            expect(collection).to be_capped
          end

          it 'creates the collection in the database' do
            expect(database.collection_names).to include('specs')
          end
        end

        context 'when instantiating a collection directly' do

          let(:collection) do
            described_class.new(database, :specs, :capped => true, :size => 1024)
          end

          it_behaves_like 'a capped collection command'
        end

        context 'when instantiating a collection through the database' do

          let(:collection) do
            authorized_client[:specs, :capped => true, :size => 1024]
          end

          it_behaves_like 'a capped collection command'
        end
      end
    end
  end

  describe '#drop' do

    let(:database) do
      authorized_client.database
    end

    let(:collection) do
      described_class.new(database, :specs)
    end

    before do
      collection.create
    end

    let!(:response) do
      collection.drop
    end

    it 'executes the command' do
      expect(response).to be_successful
    end

    it 'drops the collection from the database' do
      expect(database.collection_names).to_not include('specs')
    end

    context 'when the collection does not exist' do

      it 'does not raise an error' do
        expect(database['non-existent-coll'].drop).to be(false)
      end
    end
  end

  describe '#find' do

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

    context 'when providing a bad selector' do

      let(:view) do
        authorized_collection.find('$or' => [])
      end

      it 'raises an exception when iterating' do
        expect {
          view.to_a
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when iterating the authorized_collection view' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      after do
        authorized_collection.find.delete_many
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

  describe '#insert_many' do

    after do
      authorized_collection.find.delete_many
    end

    let(:result) do
      authorized_collection.insert_many([{ name: 'test1' }, { name: 'test2' }])
    end

    it 'inserts the documents into the collection', if: write_command_enabled? do
      expect(result.written_count).to eq(2)
    end

    it 'inserts the documents into the collection', unless: write_command_enabled? do
      expect(result.written_count).to eq(0)
    end

    it 'contains the ids in the result' do
      expect(result.inserted_ids.size).to eq(2)
    end
  end

  describe '#insert_one' do

    after do
      authorized_collection.find.delete_many
    end

    let(:result) do
      authorized_collection.insert_one({ name: 'testing' })
    end

    it 'inserts the document into the collection', if: write_command_enabled? do
      expect(result.written_count).to eq(1)
    end

    it 'inserts the document into the collection', unless: write_command_enabled? do
      expect(result.written_count).to eq(0)
    end

    it 'contains the id in the result' do
      expect(result.inserted_id).to_not be_nil
    end
  end

  describe '#inspect' do

    it 'includes the object id' do
      expect(authorized_collection.inspect).to include(authorized_collection.object_id.to_s)
    end

    it 'includes the namespace' do
      expect(authorized_collection.inspect).to include(authorized_collection.namespace)
    end
  end

  describe '#indexes' do

    let(:index_spec) do
      { name: 1 }
    end

    let(:batch_size) { nil }

    let(:index_names) do
      authorized_collection.indexes(batch_size: batch_size).collect { |i| i['name'] }
    end

    before do
      authorized_collection.indexes.create_one(index_spec, unique: true)
    end

    after do
      authorized_collection.indexes.drop_one('name_1')
    end

    it 'returns a list of indexes' do
      expect(index_names).to include(*'name_1', '_id_')
    end

    context 'when batch size is specified' do

      let(:batch_size) { 1 }

      it 'returns a list of indexes' do
        expect(index_names).to include(*'name_1', '_id_')
      end
    end
  end
end
