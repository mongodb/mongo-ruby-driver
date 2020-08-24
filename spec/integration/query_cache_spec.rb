require 'spec_helper'

describe 'QueryCache' do
  around do |spec|
    Mongo::QueryCache.clear_cache
    Mongo::QueryCache.cache { spec.run }
  end

  before do
    authorized_collection.delete_many
  end

  let(:subscriber) { EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

  describe '#cache' do

    before do
      Mongo::QueryCache.enabled = false
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    it 'enables the query cache inside the block' do
      Mongo::QueryCache.cache do
        authorized_collection.find(name: 'testing').to_a
        expect(Mongo::QueryCache.enabled?).to be(true)
        expect(Mongo::QueryCache.cache_table.length).to eq(1)
        expect(events.length).to eq(2)
      end
      authorized_collection.find(name: 'testing').to_a
      expect(Mongo::QueryCache.enabled?).to be(false)
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      expect(events.length).to eq(2)
    end
  end

  describe '#uncached' do

    before do
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    it 'disables the query cache inside the block' do
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      Mongo::QueryCache.uncached do
        authorized_collection.find(name: 'testing').to_a
        expect(Mongo::QueryCache.enabled?).to be(false)
        expect(events.length).to eq(2)
      end
      authorized_collection.find(name: 'testing').to_a
      expect(Mongo::QueryCache.enabled?).to be(true)
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      expect(events.length).to eq(2)
    end
  end

  describe 'iterating cursors multiple times' do
    before do
      authorized_collection.drop
      Mongo::QueryCache.enabled = true
    end

    after do
      Mongo::QueryCache.enabled = false
    end

    context 'when query returns single batch' do
      before do
        authorized_collection.insert_many([{ test: 1 }] * 100)
      end

      it 'does not raise an exception' do
        expect do
          authorized_collection.find(test: 1).to_a
          authorized_collection.find(test: 1).to_a
        end.not_to raise_error
      end
    end

    context 'when query returns single batch' do
      before do
        authorized_collection.insert_many([{ test: 1 }] * 2000)
      end

      it 'does not raise an exception' do
        expect do
          authorized_collection.find(test: 1).to_a
          authorized_collection.find(test: 1).to_a
        end.not_to raise_error
      end
    end
  end

  context 'when querying in the same collection' do

    before do
      10.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    context 'when query cache is disabled' do

      before do
        Mongo::QueryCache.enabled = false
        authorized_collection.find(test: 1).to_a
      end

      it 'queries again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(2)
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
      end
    end

    context 'when query cache is enabled' do

      before do
        authorized_collection.find(test: 1).to_a
      end

      it 'does not query again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(1)
        expect(Mongo::QueryCache.cache_table.length).to eq(1)
      end
    end

    context 'when query has collation' do
      min_server_fcv '3.4'

      let(:options1) do
        { :collation => { locale: 'fr' } }
      end

      let(:options2) do
        { collation: { locale: 'en_US' } }
      end

      before do
        authorized_collection.find({ test: 3 }, options1).to_a
      end

      context 'when query has the same collation' do

        it 'uses the cache' do
          authorized_collection.find({ test: 3 }, options1).to_a
          expect(events.length).to eq(1)
        end
      end

      context 'when query has a different collation' do

        it 'queries again' do
          authorized_collection.find({ test: 3 }, options2).to_a
          expect(events.length).to eq(2)
          expect(Mongo::QueryCache.cache_table.length).to eq(2)
        end
      end
    end

    context 'when first query has no limit' do

      before do
        authorized_collection.find.to_a.count
      end

      context 'when next query has a limit' do

        it 'uses the cache' do
          expect(authorized_collection.find.limit(5).to_a.count).to eq(5)
          expect(authorized_collection.find.limit(3).to_a.count).to eq(3)
          expect(authorized_collection.find.to_a.count).to eq(10)
          expect(events.length).to eq(1)
        end
      end
    end

    context 'when first query has a limit' do

      before do
        authorized_collection.find.limit(2).to_a
      end

      context 'when next query has a different limit' do

        it 'queries again' do
          expect(authorized_collection.find.limit(3).to_a.count).to eq(3)
          expect(events.length).to eq(2)
        end
      end

      context 'when next query does not have a limit' do

        it 'queries again' do
          expect(authorized_collection.find.to_a.count).to eq(10)
          expect(events.length).to eq(2)
        end
      end
    end

    context 'when querying only the first' do

      before do
        5.times do |i|
          authorized_collection.insert_one(test: 11)
        end
      end

      before do
        authorized_collection.find({test: 11}).to_a
      end

      it 'does not query again' do
        expect(authorized_collection.find({test: 11}).count).to eq(5)
        authorized_collection.find({test: 11}).first
        expect(events.length).to eq(1)
      end

      context 'when limiting the result' do

        it 'does not query again' do
          authorized_collection.find({test: 11}, limit: 2).to_a
          expect(authorized_collection.find({test: 11}, limit: 2).to_a.count).to eq(2)
          expect(events.length).to eq(1)
        end
      end
    end

    context 'when specifying a different skip value' do

      before do
        authorized_collection.find({}, {limit: 2, skip: 3}).to_a
      end

      it 'queries again' do
        results = authorized_collection.find({}, {limit: 2, skip: 5}).to_a
        expect(results.count).to eq(2)
        expect(events.length).to eq(2)
      end
    end

    context 'when sorting documents' do

      before do
        authorized_collection.find({}, desc).to_a
      end

      let(:desc) do
        { sort: {test: -1} }
      end

      let(:asc) do
        { sort: {test: 1} }
      end

      context 'with different selector' do

        it 'queries again' do
          authorized_collection.find({}, asc).to_a
          expect(events.length).to eq(2)
        end
      end

      it 'does not query again' do
        authorized_collection.find({}, desc).to_a
        expect(events.length).to eq(1)
      end
    end

    context 'when inserting new documents' do

      before do
        authorized_collection.find.to_a
        authorized_collection.insert_one({ name: "bob" })
      end

      it 'queries again' do
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end
    end

    context 'when deleting documents' do

      before do
        authorized_collection.find.to_a
        authorized_collection.delete_many
      end

      it 'queries again' do
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end
    end

    context 'when replacing documents' do
      before do
        authorized_collection.find.to_a
        authorized_collection.replace_one(selector, { test: 100 } )
      end

      let(:selector) do
        { test: 5 }
      end

      it 'queries again' do
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end
    end
  end

  context 'when find command fails and retries' do
    require_fail_command
    require_no_multi_shard

    before do
      5.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    before do
      client.use('admin').command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: {
          failCommands: ['find'],
          closeConnection: true
        }
      )
    end

    let(:command_name) { 'find' }

    it 'uses modern retryable reads when using query cache' do
      expect(Mongo::QueryCache.enabled?).to be(true)

      expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
      authorized_collection.find(test: 1).to_a
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      expect(subscriber.command_started_events('find').length).to eq(2)

      authorized_collection.find(test: 1).to_a
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      expect(subscriber.command_started_events('find').length).to eq(2)
    end
  end

  context 'when querying in a different collection' do

    let(:database) { client.database }

    let(:new_collection) do
      Mongo::Collection.new(database, 'foo')
    end

    before do
      authorized_collection.find.to_a
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    it 'queries again' do
      new_collection.find.to_a
      expect(Mongo::QueryCache.cache_table.length).to eq(2)
      expect(events.length).to eq(2)
    end
  end
end
