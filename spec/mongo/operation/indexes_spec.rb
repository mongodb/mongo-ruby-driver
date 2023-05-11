# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Indexes do
  require_no_required_api_version

  let(:context) { Mongo::Operation::Context.new }

  describe '#execute' do

    let(:index_spec) do
      { name: 1 }
    end

    before do
      authorized_collection.drop
      authorized_collection.insert_one(test: 1)
      authorized_collection.indexes.create_one(index_spec, unique: true)
    end

    after do
      authorized_collection.indexes.drop_one('name_1')
    end

    let(:operation) do
      described_class.new({ selector: { listIndexes: TEST_COLL },
                            coll_name: TEST_COLL,
                            db_name: SpecConfig.instance.test_db })
    end

    let(:indexes) do
      operation.execute(authorized_primary, context: context)
    end

    it 'returns the indexes for the collection' do
      expect(indexes.documents.size).to eq(2)
    end
  end
end
