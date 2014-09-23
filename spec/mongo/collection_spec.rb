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

    context 'when iterating the authorized_collection view' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      after do
        authorized_collection.find.remove_many
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
      authorized_collection.find.remove_many
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
  end

  describe '#insert_one' do

    after do
      authorized_collection.find.remove_many
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
  end

  describe '#parallel_scan' do

    let(:documents) do
      (1..100).map do |i|
        { name: "testing-scan-#{i}" }
      end
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.find.remove_many
    end

    let(:result) do
      authorized_collection.parallel_scan(2)
    end

    it 'returns a multi-cursor for all documents' do
      result.each do |doc|
        expect(doc[:name]).to_not be_nil
      end
    end

    it 'returns the correct number of documents' do
      expect(result.count).to eq(100)
    end
  end

end
