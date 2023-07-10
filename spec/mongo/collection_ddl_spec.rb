# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection do

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

  before do
    authorized_client['collection_spec'].drop
  end

  describe '#create' do
    before do
      authorized_client[:specs].drop
    end

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

          let(:options) do
            { :capped => true, :size => 1024 }
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

        shared_examples 'a validated collection command' do

          let!(:response) do
            collection.create
          end

          let(:options) do
            { :validator => { fieldName: { '$gte' =>  1024 } },
              :validationLevel => 'strict' }
          end

          let(:collection_info) do
            database.list_collections.find { |i| i['name'] == 'specs' }
          end

          it 'executes the command' do
            expect(response).to be_successful
          end

          it 'sets the collection with validators' do
            expect(collection_info['options']['validator']).to eq({ 'fieldName' => { '$gte' => 1024 } })
          end

          it 'creates the collection in the database' do
            expect(database.collection_names).to include('specs')
          end
        end

        context 'when instantiating a collection directly' do

          let(:collection) do
            described_class.new(database, :specs, options)
          end

          it_behaves_like 'a capped collection command'
          it_behaves_like 'a validated collection command'
        end

        context 'when instantiating a collection through the database' do

          let(:collection) do
            authorized_client[:specs, options]
          end

          it_behaves_like 'a capped collection command'
          it_behaves_like 'a validated collection command'
        end

        context 'when instantiating a collection using create' do

          before do
            authorized_client[:specs].drop
          end

          let!(:response) do
            authorized_client[:specs].create(options)
          end

          let(:collection) do
            authorized_client[:specs]
          end

          let(:collstats) do
            collection.aggregate([ {'$collStats' => { 'storageStats' => {} }} ]).first
          end

          let(:storage_stats) do
            collstats.fetch('storageStats', {})
          end

          let(:options) do
            { :capped => true, :size => 4096, :max => 512 }
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

          it "applies the options" do
            expect(storage_stats["capped"]).to be true
            expect(storage_stats["max"]).to eq(512)
            expect(storage_stats["maxSize"]).to eq(4096)
          end
        end
      end

      context 'when the collection has a write concern' do

        before do
          database[:specs].drop
        end

        let(:options) do
          {
            write: INVALID_WRITE_CONCERN
          }
        end

        let(:collection) do
          described_class.new(database, :specs, options)
        end

        context 'when the server supports write concern on the create command' do
          require_topology :replica_set

          it 'applies the write concern' do
            expect{
              collection.create
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when write concern passed in as an option' do
          require_topology :replica_set

          before do
            database['collection_spec'].drop
          end

          let(:events) do
            subscriber.command_started_events('create')
          end

          let(:options) do
            { write_concern: {w: 1} }
          end

          let!(:collection) do
            authorized_collection.with(options)
          end

          let!(:command) do
            Utils.get_command_event(authorized_client, 'create') do |client|
              collection.create({ write_concern: {w: 2} })
            end.command
          end

          it 'applies the write concern passed in as an option' do
            expect(events.length).to eq(1)
            expect(command[:writeConcern][:w]).to eq(2)
          end
        end
      end

      context 'when the collection has a collation' do

        shared_examples 'a collection command with a collation option' do

          let(:response) do
            collection.create
          end

          let(:options) do
            { :collation => { locale: 'fr' } }
          end

          let(:collection_info) do
            database.list_collections.find { |i| i['name'] == 'specs' }
          end

          before do
            collection.drop
          end

          it 'executes the command' do
            expect(response).to be_successful
          end

          it 'sets the collection with a collation' do
            response
            expect(collection_info['options']['collation']['locale']).to eq('fr')
          end

          it 'creates the collection in the database' do
            response
            expect(database.collection_names).to include('specs')
          end
        end

        context 'when instantiating a collection directly' do

          let(:collection) do
            described_class.new(database, :specs, options)
          end

          it_behaves_like 'a collection command with a collation option'
        end

        context 'when instantiating a collection through the database' do

          let(:collection) do
            authorized_client[:specs, options]
          end

          it_behaves_like 'a collection command with a collation option'
        end

        context 'when passing the options through create' do

          let(:collection) do
            authorized_client[:specs]
          end

          let(:response) do
            collection.create(options)
          end

          let(:options) do
            { :collation => { locale: 'fr' } }
          end

          let(:collection_info) do
            database.list_collections.find { |i| i['name'] == 'specs' }
          end

          before do
            collection.drop
          end

          it 'executes the command' do
            expect(response).to be_successful
          end

          it 'sets the collection with a collation' do
            response
            expect(collection_info['options']['collation']['locale']).to eq('fr')
          end

          it 'creates the collection in the database' do
            response
            expect(database.collection_names).to include('specs')
          end
        end
      end

      context 'when a session is provided' do

        let(:collection) do
          authorized_client[:specs]
        end

        let(:operation) do
          collection.create(session: session)
        end

        let(:session) do
          authorized_client.start_session
        end

        let(:client) do
          authorized_client
        end

        let(:failed_operation) do
          authorized_client[:specs, invalid: true].create(session: session)
        end

        before do
          collection.drop
        end

        it_behaves_like 'an operation using a session'
        it_behaves_like 'a failed operation using a session'
      end
    end

    context 'when collation has a strength' do

      let(:band_collection) do
        described_class.new(database, :bands)
      end

      before do
        band_collection.delete_many
        band_collection.insert_many([{ name: "Depeche Mode" }, { name: "New Order" }])
      end

      let(:options) do
        { collation: { locale: 'en_US', strength: 2 } }
      end
      let(:band_result) do
        band_collection.find({ name: 'DEPECHE MODE' }, options)
      end

      it 'finds Capitalize from UPPER CASE' do
        expect(band_result.count_documents).to eq(1)
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

    context 'when the collection exists' do

      before do
        authorized_client[:specs].drop
        collection.create
        # wait for the collection to be created
        sleep 0.4
      end

      context 'when a session is provided' do

        let(:operation) do
          collection.drop(session: session)
        end

        let(:failed_operation) do
          collection.with(write: INVALID_WRITE_CONCERN).drop(session: session)
        end

        let(:session) do
          authorized_client.start_session
        end

        let(:client) do
          authorized_client
        end

        it_behaves_like 'an operation using a session'

        context 'can set write concern' do
          require_set_write_concern

          it_behaves_like 'a failed operation using a session'
        end
      end

      context 'when the collection does not have a write concern set' do

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
          require_set_write_concern
          max_server_fcv '6.99.99'

          it 'does not raise an error' do
            expect(database['non-existent-coll'].drop).to be(false)
          end
        end
      end

      context 'when the collection has a write concern' do

        let(:write_options) do
          {
            write: INVALID_WRITE_CONCERN
          }
        end

        let(:collection_with_write_options) do
          collection.with(write_options)
        end

        context 'when the server supports write concern on the drop command' do
          require_set_write_concern

          it 'applies the write concern' do
            expect{
              collection_with_write_options.drop
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end

        context 'when write concern passed in as an option' do
          require_set_write_concern

          let(:events) do
            subscriber.command_started_events('drop')
          end

          let(:options) do
            { write_concern: {w: 1} }
          end

          let!(:collection) do
            authorized_collection.with(options)
          end

          let!(:command) do
            Utils.get_command_event(authorized_client, 'drop') do |client|
              collection.drop({ write_concern: {w: 0} })
            end.command
          end

          it 'applies the write concern passed in as an option' do
            expect(events.length).to eq(1)
            expect(command[:writeConcern][:w]).to eq(0)
          end
        end
      end
    end

    context 'when the collection does not exist' do
      require_set_write_concern
      max_server_fcv '6.99.99'

      before do
        begin
          collection.drop
        rescue Mongo::Error::OperationFailure
        end
      end

      it 'returns false' do
        expect(collection.drop).to be(false)
      end
    end

    context "when providing a pipeline in create" do

      let(:options) do
        { view_on: "specs", pipeline: [ { :'$project' => { "baz": "$bar" } } ] }
      end

      before do
        authorized_client["my_view"].drop
        authorized_client[:specs].drop
      end

      it "the pipeline gets passed to the command" do
        expect(Mongo::Operation::Create).to receive(:new).and_wrap_original do |m, *args|
          expect(args.first.slice(:selector)[:selector]).to have_key(:pipeline)
          expect(args.first.slice(:selector)[:selector]).to have_key(:viewOn)
          m.call(*args)
        end
        expect_any_instance_of(Mongo::Operation::Create).to receive(:execute)
        authorized_client[:specs].create(options)
      end
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

    it 'returns a list of indexes' do
      expect(index_names).to include(*'name_1', '_id_')
    end

    context 'when a session is provided' do
      require_wired_tiger

      let(:session) do
        authorized_client.start_session
      end

      let(:operation) do
        authorized_collection.indexes(batch_size: batch_size, session: session).collect { |i| i['name'] }
      end

      let(:failed_operation) do
        authorized_collection.indexes(batch_size: -100, session: session).collect { |i| i['name'] }
      end

      let(:client) do
        authorized_client
      end

      it_behaves_like 'an operation using a session'
      it_behaves_like 'a failed operation using a session'
    end

    context 'when batch size is specified' do

      let(:batch_size) { 1 }

      it 'returns a list of indexes' do
        expect(index_names).to include(*'name_1', '_id_')
      end
    end
  end
end
