require 'spec_helper'

describe Mongo::MultiCursor do

  let(:documents) do
    (1..10).map{ |i| { field: "test#{i}" }}
  end

  before do
    authorized_collection.insert_many(documents)
  end

  after do
    authorized_collection.find.remove_many
  end

  describe '#each' do

    let(:query_spec) do
      { :selector => {}, :options => {}, :db_name => TEST_DB, :coll_name => TEST_COLL }
    end

    let(:reply_one) do
      Mongo::Operation::Read::Query.new(query_spec).execute(authorized_primary.context)
    end

    let(:reply_two) do
      Mongo::Operation::Read::Query.new(query_spec).execute(authorized_primary.context)
    end

    let(:view) do
      Mongo::Collection::View.new(authorized_collection)
    end

    let(:cursor_one) do
      Mongo::Cursor.new(view, reply_one, authorized_primary)
    end

    let(:cursor_two) do
      Mongo::Cursor.new(view, reply_two, authorized_primary)
    end

    let(:multi_cursor) do
      described_class.new([ cursor_one, cursor_two ])
    end

    context 'when provided a block' do

      it 'iterates over each wrapped cursor and yields their documents' do
        multi_cursor.each do |doc|
          expect(doc).to have_key('field')
        end
      end

      it 'iterates the correct number of documents' do
        expect(multi_cursor.count).to eq(20)
      end
    end

    context 'when no block is provided' do

      let(:enum) do
        multi_cursor.each
      end

      it 'returns an enumerator' do
        expect(enum).to be_a(Enumerator)
      end

      context 'when subsequently iterating the enumerator' do

        it 'iterates over each wrapped cursor and yields their documents' do
          enum.each do |doc|
            expect(doc).to have_key('field')
          end
        end

        it 'iterates the correct number of documents' do
          expect(enum.count).to eq(20)
        end
      end
    end
  end
end
