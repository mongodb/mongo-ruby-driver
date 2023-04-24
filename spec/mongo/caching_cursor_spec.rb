# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::CachingCursor do

  around do |spec|
    Mongo::QueryCache.clear
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

  let(:view) do
    Mongo::Collection::View.new(authorized_collection)
  end

  before do
    authorized_collection.delete_many
    3.times { |i| authorized_collection.insert_one(_id: i) }
  end

  describe '#cached_docs' do
    context 'when no query has been performed' do
      it 'returns nil' do
        expect(cursor.cached_docs).to be_nil
      end
    end

    context 'when a query has been performed' do
      it 'returns the number of documents' do
        cursor.to_a
        expect(cursor.cached_docs.length).to eq(3)
        expect(cursor.cached_docs).to eq([{ '_id' => 0 }, { '_id' => 1 }, { '_id' => 2 }])
      end
    end
  end

  describe '#try_next' do
    it 'fetches the next document' do
      expect(cursor.try_next).to eq('_id' => 0)
      expect(cursor.try_next).to eq('_id' => 1)
      expect(cursor.try_next).to eq('_id' => 2)
    end
  end

  describe '#each' do
    it 'iterates the cursor' do
      result = cursor.each.to_a
      expect(result.length).to eq(3)
      expect(result).to eq([{ '_id' => 0 }, { '_id' => 1 }, { '_id' => 2 }])
    end
  end
end
