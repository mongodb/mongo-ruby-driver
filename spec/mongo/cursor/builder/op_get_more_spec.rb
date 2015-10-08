require 'spec_helper'

describe Mongo::Cursor::Builder::OpGetMore do

  describe '#specification' do

    let(:reply) do
      Mongo::Protocol::Reply.allocate
    end

    let(:result) do
      Mongo::Operation::Result.new(reply)
    end

    let(:view) do
      Mongo::Collection::View.new(
        authorized_collection,
        {},
        tailable: true,
        max_time_ms: 100
      )
    end

    let(:cursor) do
      Mongo::Cursor.new(view, result, authorized_primary)
    end

    let(:builder) do
      described_class.new(cursor)
    end

    let(:specification) do
      builder.specification
    end

    it 'includes to return' do
      expect(specification[:to_return]).to eq(0)
    end

    it 'includes the cursor id' do
      expect(specification[:cursor_id]).to eq(cursor.id)
    end

    it 'includes the database name' do
      expect(specification[:db_name]).to eq(TEST_DB)
    end

    it 'includes the collection name' do
      expect(specification[:coll_name]).to eq(TEST_COLL)
    end
  end
end
