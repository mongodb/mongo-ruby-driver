require 'spec_helper'

describe Mongo::CachingCursor do

  around do |spec|
    Mongo::QueryCache.clear_cache
    Mongo::QueryCache.cache { spec.run }
  end

  let(:authorized_collection) do
    authorized_client['caching_cursor']
  end

  before do
    authorized_collection.drop
  end

  let(:server) do
    view.send(:server_selector).select_server(authorized_client.cluster)
  end

  let(:reply) do
    view.send(:send_initial_query, server)
  end

  let(:cursor) do
    described_class.new(view, reply, server)
  end

  context 'when query cache is enabled' do

    let(:view) do
      Mongo::Collection::View.new(authorized_collection)
    end

    let(:documents) do
      (1..3).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    it 'caches the result documents' do
      expect(cursor.cached_docs).to be_nil
      expect(cursor.to_a.count).to eq(3)
      expect(cursor.cached_docs.count).to eq(3)

      expect(cursor.to_a.count).to eq(3)
      expect(cursor.cached_docs.count).to eq(3)
    end
  end

  context 'when iterating first time' do

    before do
      3.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    let(:view) do
      Mongo::Collection::View.new(authorized_collection, {},
        sort: {test: 1}, projection: {_id: 0}, batch_size: 2)
    end

    it 'supports try_next' do
      expect(cursor.try_next).to eq('test' => 0)
      expect(cursor.try_next).to eq('test' => 1)
    end

    it 'supports each' do
      expect(cursor.each.to_a.length).to eq(3)
    end
  end
end
