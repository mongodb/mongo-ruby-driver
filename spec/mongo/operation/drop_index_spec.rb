# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::DropIndex do
  require_no_required_api_version

  before do
    authorized_collection.indexes.drop_all
  end

  let(:context) { Mongo::Operation::Context.new }

  describe '#execute' do

    context 'when the index exists' do

      let(:spec) do
        { another: -1 }
      end

      before do
        authorized_collection.indexes.create_one(spec, unique: true)
      end

      let(:operation) do
        described_class.new(
          db_name: SpecConfig.instance.test_db,
          coll_name: TEST_COLL,
          index_name: 'another_-1'
        )
      end

      let(:response) do
        operation.execute(authorized_primary, context: context)
      end

      it 'removes the index' do
        expect(response).to be_successful
      end
    end

    context 'when the index does not exist' do

      let(:operation) do
        described_class.new(
          db_name: SpecConfig.instance.test_db,
          coll_name: TEST_COLL,
          index_name: 'another_blah'
        )
      end

      it 'raises an exception' do
        expect {
          operation.execute(authorized_primary, context: context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
