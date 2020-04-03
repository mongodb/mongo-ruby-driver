require 'spec_helper'

describe Mongo::Cursor::Builder::OpKillCursors do

  let(:reply) do
    Mongo::Protocol::Reply.allocate.tap do |reply|
      allow(reply).to receive(:cursor_id).and_return(8000)
    end
  end

  let(:description) do
    Mongo::Server::Description.new(double('description address'),
      'minWireVersion' => 0, 'maxWireVersion' => 2)
  end

  let(:result) do
    Mongo::Operation::Result.new(reply, description)
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

  describe '#specification' do

    let(:specification) do
      builder.specification
    end

    it 'includes the cursor ids' do
      expect(specification[:cursor_ids]).to eq([BSON::Int64.new(8000)])
    end

    it 'includes the database name' do
      expect(specification[:db_name]).to eq(SpecConfig.instance.test_db)
    end

    it 'includes the collection name' do
      expect(specification[:coll_name]).to eq(TEST_COLL)
    end
  end

  describe '.get_cursors_list' do
    it 'returns integer cursor ids' do
      expect(described_class.get_cursors_list(builder.specification)).to eq([8000])
    end
  end
end
