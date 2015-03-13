require 'spec_helper'

describe Mongo::Operation::Write::DropIndex do

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
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          index_name: 'another_-1'
        )
      end

      let(:response) do
        operation.execute(authorized_primary.context)
      end

      it 'removes the index' do
        expect(response).to be_successful
      end
    end

    context 'when the index does not exist' do

      let(:operation) do
        described_class.new(
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          index_name: 'another_blah'
        )
      end

      it 'raises an exception' do
        expect {
          operation.execute(authorized_primary.context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
