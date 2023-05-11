# frozen_string_literal: true
# rubocop:todo all

# TODO convert, move or delete these tests as part of RUBY-2706.

=begin
require 'spec_helper'

describe Mongo::Cursor::Builder::OpGetMore do

  describe '#specification' do

    let(:reply) do
      Mongo::Protocol::Reply.allocate.tap do |reply|
        allow(reply).to receive(:cursor_id).and_return(8000)
      end
    end

    let(:description) do
      Mongo::Server::Description.new(
        double('description address'),
        { 'minWireVersion' => 0, 'maxWireVersion' => 2 }
      )
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

    let(:specification) do
      builder.specification
    end

    it 'includes to return' do
      expect(specification[:to_return]).to eq(0)
    end

    it 'includes the cursor id' do
      expect(specification[:cursor_id]).to eq(BSON::Int64.new(8000))
    end

    it 'includes the database name' do
      expect(specification[:db_name]).to eq(SpecConfig.instance.test_db)
    end

    it 'includes the collection name' do
      expect(specification[:coll_name]).to eq(TEST_COLL)
    end
  end
end
=end
