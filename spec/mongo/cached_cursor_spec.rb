require 'spec_helper'

describe Mongo::CachedCursor do

  let(:authorized_collection) do
    authorized_client['cached_cursor']
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

  context 'when query cache enabled documents are cached' do

    let(:view) do
      Mongo::Collection::View.new(authorized_collection)
    end

    let(:documents) do
      (1..3).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    context 'when query cache enabled' do

      before do
        Mongo::QueryCache.enabled = true
      end

      it 'docs are cached when query cache enabled and query is repeated' do
        expect(cursor.get_cached_docs).to be_nil
        expect(cursor.to_a.count).to eq(3)
        expect(cursor.get_cached_docs.count).to eq(3)

        expect(cursor.to_a.count).to eq(3)
        expect(cursor.get_cached_docs.count).to eq(3)
      end
    end
  end

  context 'when query has results' do

    before do
      3.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    let(:view) do
      Mongo::Collection::View.new(authorized_collection, {},
        sort: {test: 1}, projection: {_id: 0}, batch_size: 2)
    end

    context 'when iterating first time' do
      it 'supports try_next' do
        expect(cursor.try_next).to eq('test' => 0)
        expect(cursor.try_next).to eq('test' => 1)
      end

      it 'supports each' do
        expect(cursor.each.to_a.length).to eq(3)
      end
    end
  end
end
