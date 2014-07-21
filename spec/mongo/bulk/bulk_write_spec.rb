require 'spec_helper'

describe Mongo::Bulk::BulkWrite do

  let(:write_concern) { Mongo::WriteConcern::Mode.get(:w => 1) }
  let(:database) { Mongo::Database.new(double('client'), :test) }
  let(:collection) do
    Mongo::Collection.new(database, 'users').tap do |c|
      allow(c).to receive(:write_concern) { write_concern }
    end
  end
  let(:response) { {} }
  let(:failed_op) do
    double('op').tap do |op|
      allow(op).to receive(:execute) { response }
    end
  end

  context 'ordered' do
    let(:bulk) { described_class.new(collection, :ordered => true) }

    it_behaves_like 'a bulk write object'

    context 'w = 0' do
      let(:expected_count) { 1 }
      let(:write_concern) do
        { :w => 0 }
      end
      before do
        bulk.insert(:_id => 1)
        bulk.insert(:_id => 1)
      end

      it 'does not report write concern errors' do
        #expect{ bulk.execute(write_concern) }.not_to raise_exception
        #expect(collection.count).to eq(expected_count)
      end
    end
  end

  context 'unordered' do
    let(:bulk) { described_class.new(collection, :ordered => false) }

    it_behaves_like 'a bulk write object'

    context 'wtimeout and duplicate key error' do
      before do
        # 2-node replica set
        #add unique index to collection on a field, :a
        #bulk.insert(:a => 1)
        #bulk.find(:a => 1).upsert.update('$set' => {:a => 2})
        #bulk.insert(:a => 2)
      end

      it 'raises an exception' do
        #expect{ ex = bulk.execute }.to raise_exception
        #expect(ex['ok']).to eq(1)
        #expect(ex['n']).to eq(2)
        #expect(ex['writeErrors'].first['errmsg']).to match(/duplicate key error/)
      end
    end

    context 'w = 0' do
      let(:expected_count) { 2 }
      let(:write_concern) do
        { :w => 0 }
      end
      before do
        bulk.insert(:_id => 1)
        bulk.insert(:_id => 1)
      end

      it 'does not report write concern errors' do
        #expect{ bulk.execute(write_concern) }.not_to raise_exception
        #expect(collection.count).to eq(expected_count)
      end
    end
  end
end
