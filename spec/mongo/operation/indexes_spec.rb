require 'spec_helper'

describe Mongo::Operation::Indexes do
  before do
    begin
      authorized_collection.delete_many
    rescue Mongo::Error::OperationFailure
    end
    begin
      authorized_collection.indexes.drop_all
    rescue Mongo::Error::OperationFailure
    end
  end

  describe '#execute' do

    let(:index_spec) do
      { name: 1 }
    end

    before do
      authorized_collection.indexes.create_one(index_spec, unique: true)
    end

    after do
      authorized_collection.indexes.drop_one('name_1')
    end

    let(:operation) do
      described_class.new({ selector: { listIndexes: TEST_COLL },
                            coll_name: TEST_COLL,
                            db_name: TEST_DB })
    end

    let(:indexes) do
      operation.execute(authorized_primary)
    end

    it 'returns the indexes for the collection' do
      expect(indexes.documents.size).to eq(2)
    end
  end
end
