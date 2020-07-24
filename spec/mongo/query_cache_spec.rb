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

  let(:events) do
    subscriber.command_started_events('find')
  end

  describe '#enabled' do

    context 'when query cache is disabled' do

      before do
        Mongo::QueryCache.enabled = false
      end

      it 'disables the query cache' do
        expect(Mongo::QueryCache.enabled?).to be(false)
      end
    end

    context 'when query cache is enabled' do

      before do
        Mongo::QueryCache.enabled = true
      end

      it 'enables the query cache' do
        expect(Mongo::QueryCache.enabled?).to be(true)
      end
    end
  end

  describe '#cache' do

    before do
      Mongo::QueryCache.enabled = false
    end

    it 'enables the query cache inside the block' do
      Mongo::QueryCache.cache do
        expect(Mongo::QueryCache.enabled?).to be(true)
      end
      expect(Mongo::QueryCache.enabled?).to be(false)
    end
  end

  describe '#uncached' do

    it 'disables the query cache inside the block' do
      Mongo::QueryCache.uncached do
        expect(Mongo::QueryCache.enabled?).to be(false)
      end
      expect(Mongo::QueryCache.enabled?).to be(true)
    end
  end

  describe '#cache_table' do

    before do
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    it 'gets the cached query' do
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      authorized_collection.find(name: 'testing').to_a
      expect(events.length).to eq(1)
    end
  end

  describe '#clear_cache' do

    before do
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    it 'clears the cache' do
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      Mongo::QueryCache.clear_cache
      expect(Mongo::QueryCache.cache_table.length).to eq(0)
    end
  end
end
