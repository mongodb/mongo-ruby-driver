require 'spec_helper'

describe Mongo::Collection do

  after do
    authorized_collection.delete_many
  end

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

    context 'when provided options' do

      let(:view) do
        authorized_collection.find({}, options)
      end

      context 'when provided :allow_partial_results' do

        let(:options) do
          { allow_partial_results: true }
        end

        it 'returns a view with :allow_partial_results set' do
          expect(view.options[:allow_partial_results]).to be(options[:allow_partial_results])
        end
      end

      context 'when provided :batch_size' do

        let(:options) do
          { batch_size: 100 }
        end

        it 'returns a view with :batch_size set' do
          expect(view.options[:batch_size]).to be(options[:batch_size])
        end
      end

      context 'when provided :comment' do

        let(:options) do
          { comment: 'slow query' }
        end

        it 'returns a view with :comment set' do
          expect(view.options[:comment]).to be(options[:comment])
        end
      end

      context 'when provided :cursor_type' do

        let(:options) do
          { cursor_type: :tailable }
        end

        it 'returns a view with :cursor_type set' do
          expect(view.options[:cursor_type]).to be(options[:cursor_type])
        end
      end

      context 'when provided :max_time_ms' do

        let(:options) do
          { max_time_ms: 500 }
        end

        it 'returns a view with :max_time_ms set' do
          expect(view.options[:max_time_ms]).to be(options[:max_time_ms])
        end
      end

      context 'when provided :modifiers' do

        let(:options) do
          { modifiers: { :$orderby => Mongo::Index::ASCENDING } }
        end

        it 'returns a view with modifiers set' do
          expect(view.options[:modifiers]).to be(options[:modifiers])
        end
      end

      context 'when provided :no_cursor_timeout' do

        let(:options) do
          { no_cursor_timeout: true }
        end

        it 'returns a view with :no_cursor_timeout set' do
          expect(view.options[:no_cursor_timeout]).to be(options[:no_cursor_timeout])
        end
      end

      context 'when provided :oplog_replay' do

        let(:options) do
          { oplog_replay: false }
        end

        it 'returns a view with :oplog_replay set' do
          expect(view.options[:oplog_replay]).to be(options[:oplog_replay])
        end
      end

      context 'when provided :projection' do

        let(:options) do
          { projection:  { 'x' => 1 } }
        end

        it 'returns a view with :projection set' do
          expect(view.options[:projection]).to be(options[:projection])
        end
      end

      context 'when provided :skip' do

        let(:options) do
          { skip:  5 }
        end

        it 'returns a view with :skip set' do
          expect(view.options[:skip]).to be(options[:skip])
        end
      end

      context 'when provided :sort' do

        let(:options) do
          { sort:  { 'x' => Mongo::Index::ASCENDING } }
        end

        it 'returns a view with :sort set' do
          expect(view.options[:sort]).to be(options[:sort])
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

    it 'inserts the documents into the collection' do
      expect(result.inserted_count).to eq(2)
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

  describe '#aggregate' do

    it 'returns an Aggregation object' do
      expect(authorized_collection.aggregate([])).to be_a(Mongo::Collection::View::Aggregation)
    end

    context 'when options are provided' do

      let(:options) do
        { :allow_disk_use => true }
      end

      it 'sets the options on the Aggregation object' do
        expect(authorized_collection.aggregate([], options).options).to eq(options)
      end
    end
  end

  describe '#count' do

    let(:documents) do
      (1..10).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.find.delete_many
    end

    it 'returns an integer count' do
      expect(authorized_collection.count).to eq(10)
    end

    context 'when options are provided' do

      it 'passes the options to the count' do
        expect(authorized_collection.count({}, limit: 5)).to eq(5)
      end
    end
  end

  describe '#distinct' do

    let(:documents) do
      (1..3).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.find.delete_many
    end

    it 'returns the distinct values' do
      expect(authorized_collection.distinct(:field)).to eq([ 'test1', 'test2', 'test3' ])
    end

    context 'when a selector is provided' do

      it 'returns the distinct values' do
        expect(authorized_collection.distinct(:field, field: 'test1')).to eq([ 'test1' ])
      end
    end

    context 'when options are provided' do

      it 'passes the options to the distinct command' do
        expect(authorized_collection.distinct(:field, {}, max_time_ms: 100)).to eq([ 'test1', 'test2', 'test3' ])
      end
    end
  end

  describe '#delete_one' do

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

      after do
        authorized_collection.delete_many
      end

      let(:response) do
        authorized_collection.delete_one(selector)
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
        authorized_collection.delete_one
      end

      it 'deletes the first document in the collection' do
        expect(response.written_count).to eq(1)
      end
    end
  end

  describe '#delete_many' do

    before do
      authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
    end

    after do
      authorized_collection.delete_many
    end

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      it 'deletes the matching documents in the collection' do
        expect(authorized_collection.delete_many(selector).written_count).to eq(1)
      end
    end

    context 'when no selector was provided' do

      it 'deletes all the documents in the collection' do
        expect(authorized_collection.delete_many.written_count).to eq(2)
      end
    end
  end

  describe '#replace_one' do

    let(:selector) do
      { field: 'test1' }
    end

    context 'when a selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      after do
        authorized_collection.delete_many
      end

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'testing' })
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

    context 'when upsert is false' do

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'test1' }, upsert: false)
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').to_a
      end

      it 'reports that no documents were written' do
        expect(response.written_count).to eq(0)
      end

      it 'does not insert the document' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'test1' }, upsert: true)
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').first
      end

      after do
        authorized_collection.delete_many
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts the document' do
        expect(updated[:field]).to eq('test1')
      end
    end

    context 'when upsert is not specified' do

      let!(:response) do
        authorized_collection.replace_one(selector, { field: 'test1' })
      end

      let(:updated) do
        authorized_collection.find(field: 'test1').to_a
      end

      it 'reports that no documents were written' do
        expect(response.written_count).to eq(0)
      end

      it 'does not insert the document' do
        expect(updated).to be_empty
      end
    end
  end

  describe '#update_many' do

    let(:selector) do
      { field: 'test' }
    end

    after do
      authorized_collection.delete_many
    end

    context 'when a selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test' }, { field: 'test' }])
      end

      let!(:response) do
        authorized_collection.update_many(selector, '$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').to_a.last
      end

      it 'returns the number updated' do
        expect(response.written_count).to eq(2)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is false' do

      let(:response) do
        authorized_collection.update_many(selector, { '$set'=> { field: 'testing' } },
                         upsert: false)
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        authorized_collection.update_many(selector, { '$set'=> { field: 'testing' } },
                         upsert: true)
      end

      let(:updated) do
        authorized_collection.find.to_a.last
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts a document into the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is not specified' do

      let(:response) do
        authorized_collection.update_many(selector, { '$set'=> { field: 'testing' } })
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end
  end

  describe '#update_one' do

    let(:selector) do
      { field: 'test1' }
    end

    context 'when a selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
      end

      let!(:response) do
        authorized_collection.update_one(selector, '$set'=> { field: 'testing' })
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

    context 'when upsert is false' do

      let(:response) do
        authorized_collection.update_one(selector, { '$set'=> { field: 'testing' } },
                        upsert: false)
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end

    context 'when upsert is true' do

      let!(:response) do
        authorized_collection.update_one(selector, { '$set'=> { field: 'testing' } },
                        upsert: true)
      end

      let(:updated) do
        authorized_collection.find.first
      end

      it 'reports that a document was written' do
        expect(response.written_count).to eq(1)
      end

      it 'inserts a document into the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when upsert is not specified' do

      let(:response) do
        authorized_collection.update_one(selector, { '$set'=> { field: 'testing' } })
      end

      let(:updated) do
        authorized_collection.find.to_a
      end

      it 'reports that no documents were updated' do
        expect(response.written_count).to eq(0)
      end

      it 'updates no documents in the collection' do
        expect(updated).to be_empty
      end
    end
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
          authorized_collection.find_one_and_delete(selector)
        end

        it 'deletes the document from the database' do
          expect(authorized_collection.find.to_a).to be_empty
        end

        it 'returns the document' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when a projection is provided' do

        let!(:document) do
          authorized_collection.find_one_and_delete(selector, projection: { _id: 1 })
        end

        it 'deletes the document from the database' do
          expect(authorized_collection.find.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to be_nil
          expect(document['_id']).to_not be_nil
        end
      end

      context 'when a sort is provided' do

        let!(:document) do
          authorized_collection.find_one_and_delete(selector, sort: { field: 1 })
        end

        it 'deletes the document from the database' do
          expect(authorized_collection.find.to_a).to be_empty
        end

        it 'returns the document with limited fields' do
          expect(document['field']).to eq('test1')
        end
      end

      context 'when max_time_ms is provided' do

        it 'includes the max_time_ms value in the command' do
          expect {
            authorized_collection.find_one_and_delete(selector, max_time_ms: 0.1)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when no matching document is found' do

      let(:selector) do
        { field: 'test5' }
      end

      let!(:document) do
        authorized_collection.find_one_and_delete(selector)
      end

      it 'returns nil' do
        expect(document).to be_nil
      end
    end
  end
end
