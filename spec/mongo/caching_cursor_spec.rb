require 'spec_helper'

describe Mongo::CachingCursor do
  let(:authorized_collection) do
    authorized_client['caching_cursor']
  end

  before do
    authorized_collection.with(write_concern: {w: :majority}).delete_many
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

  describe 'iteration integration test' do

    context 'when query has results' do

      before do
        10.times do |i|
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
          expect(cursor.each.to_a.length).to eq(10)
        end
      end

      context 'when iterating second time' do
        before do
          authorized_collection.count_documents({}).should == 10
        end

        before do
          cursor.to_a
        end

        it 'does not support try_next' do
          expect do
            expect(cursor.try_next).to eq('test' => 0)
          end.to raise_error(Mongo::Error::InvalidCursorOperation, /Cannot call try_next on a caching cursor past initial iteration/)
        end

        it 'supports each' do
          expect(cursor.each.to_a.length).to eq(10)
        end
      end
    end

    context 'when query has no results' do
      let(:view) do
        Mongo::Collection::View.new(authorized_collection, {},
          sort: {test: 1}, projection: {_id: 0}, batch_size: 2)
      end

      context 'when iterating first time' do
        it 'supports try_next which raises StopIteration immediately' do
          expect do
            cursor.try_next
          end.to raise_error(StopIteration)
        end

        it 'supports each' do
          expect(cursor.each.to_a.length).to eq(0)
        end
      end

      context 'when iterating second time' do
        before do
          cursor.to_a
        end

        it 'does not support try_next' do
          expect do
            expect(cursor.try_next).to eq('test' => 0)
          end.to raise_error(Mongo::Error::InvalidCursorOperation, /Cannot call try_next on a caching cursor past initial iteration/)
        end

        it 'supports each' do
          expect(cursor.each.to_a.length).to eq(0)
        end
      end
    end
  end
end
