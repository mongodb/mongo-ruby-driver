require 'spec_helper'

describe Mongo::QueryCache do

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

  describe '#enabled' do

    context 'when query cache is disabled' do

      before do
        Mongo::QueryCache.enabled = false
      end

      it 'query cache is disabled' do
        expect(Mongo::QueryCache.enabled?).to be(false)
      end
    end
  end

  describe '#cache' do

    context 'when a block is uncached' do

      before do
        authorized_collection.insert_one({ name: 'testing' })
      end

      it 'block is executed with disabled query cache' do
        Mongo::QueryCache.cache {
          authorized_collection.find(name: 'testing')
          expect(Mongo::QueryCache.enabled?).to be(true)
        }
      end
    end
  end

  describe '#uncached' do

    context 'when a block is uncached' do

      before do
        authorized_collection.insert_one({ name: 'testing' })
      end

      it 'block is executed with disabled query cache' do
        Mongo::QueryCache.uncached {
          authorized_collection.find(name: 'testing')
          expect(Mongo::QueryCache.enabled?).to be(false)
        }
      end
    end
  end

  context 'when querying in the same collection' do

    before do
      10.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    context 'when query cache is disabled' do

      before do
        Mongo::QueryCache.enabled = false
        authorized_collection.find(test: 1).to_a
      end

      let(:events) do
        subscriber.command_started_events('find')
      end

      it 'queries again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(2)
      end
    end

    context 'when query cache is enabled' do

      before do
        authorized_collection.find(test: 1).to_a
      end

      let(:events) do
        subscriber.command_started_events('find')
      end

      it 'does not query again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(1)
      end
    end

    context 'when first query has no limit' do

      before do
        authorized_collection.find.to_a.count
      end

      let(:events) do
        subscriber.command_started_events('find')
      end

      context 'when next query has a limit' do

        it 'uses the cache' do
          authorized_collection.find({}, limit: 5).to_a.count
          expect(events.length).to eq(1)
        end
      end
    end

    context 'when first query has a limit' do

      before do
        authorized_collection.find(limit:2).to_a
      end

      let(:events) do
        subscriber.command_started_events('find')
      end

      context 'when next query has a different limit' do

        it 'queries again' do
          authorized_collection.find(limit: 3).to_a
          expect(events.length).to eq(2)
        end
      end

      context 'when next query does not have a limit' do

        it 'queries again' do
          authorized_collection.find.to_a
          expect(events.length).to eq(2)
        end
      end
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
      authorized_collection.insert_many([{ name: "test1" }, { name: "test2" }])
      authorized_collection.find({ name: 'test1' }, options1).to_a
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    it 'uses the cache for query with same collation' do
      authorized_collection.find({ name: 'test1' }, options1).to_a
      expect(events.length).to eq(1)
    end

    it 'does not use the cache for query with different collation' do
      authorized_collection.find({ name: 'test1' }, options2).to_a
      expect(events.length).to eq(2)
    end
  end

end
