require 'spec_helper'

describe Mongo::QueryCache do

  let(:subscriber) { EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

  around do |spec|
    Mongo::QueryCache.clear_cache
    Mongo::QueryCache.cache { spec.run }
  end

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

  describe '#cache_table' do
    # add tests
  end

  describe '#clear_cache' do
    # add tests
  end
end
