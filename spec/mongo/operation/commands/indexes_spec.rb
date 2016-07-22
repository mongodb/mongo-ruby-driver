require 'spec_helper'

describe Mongo::Operation::Commands::Indexes do

  describe '#execute' do

    let(:spec) do
      { name: 1 }
    end

    before do
      authorized_collection.indexes.create_one(spec, unique: true)
    end

    after do
      authorized_collection.indexes.drop_one('name_1')
    end

    let(:operation) do
      described_class.new(db_name: TEST_DB, coll_name: TEST_COLL)
    end

    let(:indexes) do
      operation.execute(authorized_primary)
    end

    it 'returns the indexes for the collection' do
      expect(indexes.documents.size).to eq(2)
    end
  end
end
